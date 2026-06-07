package ravenworks.magpie.engine.target.print;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.engine.target.TargetConnector;
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
public class PrintTargetConnector implements TargetConnector {

    private final StreamProvider provider;
    private final StreamRegistry streamRegistry;
    private final String name;
    private final String topic;
    private final List<PrintTargetWorker> workers = new ArrayList<>();

    public PrintTargetConnector(@NonNull StreamProvider provider,
                                @NonNull StreamRegistry streamRegistry,
                                @NonNull String name,
                                @NonNull String topic) {
        this.provider = provider;
        this.streamRegistry = streamRegistry;
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
            var worker = new PrintTargetWorker(this.name, i, consumers.get(i));
            this.workers.add(worker);
            worker.start();
        }
        log.info("Print target '{}' started, {} worker(s)", this.name, this.workers.size());
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        var futures = this.workers.stream()
                .map(PrintTargetWorker::shutdown)
                .toArray(CompletableFuture[]::new);
        return CompletableFuture.allOf(futures)
                .thenRun(() -> log.info("Print target '{}' shutdown", this.name));
    }


    private static class PrintTargetWorker {

        private static final int BACKPRESSURE_LIMIT = 100;

        private final String name;
        private final int partition;
        private final StreamConsumer consumer;
        private final Semaphore semaphore = new Semaphore(BACKPRESSURE_LIMIT);
        private final AtomicLong received = new AtomicLong();
        private final EventLoop eventLoop;

        PrintTargetWorker(String name, int partition, StreamConsumer consumer) {
            this.name = name;
            this.partition = partition;
            this.consumer = consumer;
            this.eventLoop = new EventLoop("tgt-" + name + "-" + partition, 5_000, this::dispatch);
        }

        void start() {
            this.consumer.consume(0, record -> {
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
