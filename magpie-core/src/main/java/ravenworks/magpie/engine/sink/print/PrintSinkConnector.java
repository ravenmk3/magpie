package ravenworks.magpie.engine.sink.print;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.engine.sink.SinkConnector;
import ravenworks.magpie.engine.sink.SinkOffsetStore;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.StreamConsumer;
import ravenworks.magpie.engine.stream.StreamDefinition;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;


/**
 * @author Raven
 */
@Slf4j
public class PrintSinkConnector implements SinkConnector {

    private final StreamProvider provider;
    private final StreamRegistry streamRegistry;
    private final SinkOffsetStore offsetStore;
    private final String name;
    private final String topic;
    private final List<PrintSinkWorker> workers = new ArrayList<>();

    public PrintSinkConnector(@NonNull StreamProvider provider,
                              @NonNull StreamRegistry streamRegistry,
                              @NonNull SinkOffsetStore offsetStore,
                              @NonNull String name,
                              @NonNull String topic) {
        this.provider = provider;
        this.streamRegistry = streamRegistry;
        this.offsetStore = offsetStore;
        this.name = name;
        this.topic = topic;
    }

    @Override
    public String type() {
        return "print";
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void start() {
        StreamDefinition definition = this.streamRegistry.getStream(this.topic);
        if (definition == null) {
            log.error("Stream not found: {}", this.topic);
            return;
        }
        var consumers = this.provider.consumer(definition, this.name);
        for (int i = 0; i < consumers.size(); i++) {
            var worker = new PrintSinkWorker(this.name, i, consumers.get(i), this.offsetStore);
            this.workers.add(worker);
            worker.start();
        }
        log.info("Print sink '{}' started, {} worker(s)", this.name, this.workers.size());
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        var futures = this.workers.stream()
                .map(PrintSinkWorker::shutdown)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenRun(() -> log.info("Print sink '{}' shutdown", this.name));
    }


    private static class PrintSinkWorker {

        private static final int BACKPRESSURE_LIMIT = 100;

        private final String name;
        private final int partition;
        private final StreamConsumer consumer;
        private final SinkOffsetStore offsetStore;
        private final Semaphore semaphore = new Semaphore(BACKPRESSURE_LIMIT);
        private final AtomicLong received = new AtomicLong();
        private final EventLoop eventLoop;

        PrintSinkWorker(String name, int partition, StreamConsumer consumer, SinkOffsetStore offsetStore) {
            this.name = name;
            this.partition = partition;
            this.consumer = consumer;
            this.offsetStore = offsetStore;
            this.eventLoop = new EventLoop("snk-" + name + "-" + partition, 5_000, this::dispatch);
        }

        void start() {
            long offset = this.offsetStore.read(this.name, this.partition);
            log.info("[{}] partition={} resuming from offset={}", this.name, this.partition, offset);
            this.consumer.consume(offset, record -> {
                this.eventLoop.enqueue(record);
                try {
                    this.semaphore.acquire();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            this.eventLoop.start();
        }

        CompletableFuture<Void> shutdown() {
            try {
                this.consumer.close();
            } catch (Exception ignored) {
            }
            return this.eventLoop.shutdown();
        }

        private void dispatch(Object event) {
            if (event instanceof ConsumerRecord record) {
                long count = this.received.incrementAndGet();
                log.info("[{}] partition={} offset={} count={} payload={}",
                        this.name, this.partition, record.getOffset(), count, new String(record.getPayload()));
                this.semaphore.release();
            }
        }

    }

}
