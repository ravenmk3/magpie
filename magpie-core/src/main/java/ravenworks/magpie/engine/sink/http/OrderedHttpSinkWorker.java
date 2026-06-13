package ravenworks.magpie.engine.sink.http;

import ravenworks.magpie.engine.sink.http.HttpSinkConnector.HttpConfig;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.StreamConsumer;

import java.time.Duration;
import java.util.List;


class OrderedHttpSinkWorker extends AbstractHttpSinkWorker {

    OrderedHttpSinkWorker(String name, int partition, StreamConsumer consumer, HttpConfig config) {
        super(name, partition, consumer, config);
    }

    @Override
    void pollAndProcess() {
        if (this.circuitBreaker.isOpen()) {
            return;
        }
        var batch = this.consumer.poll(BATCH_SIZE, Duration.ofMillis(50));
        long lastCompletedOffset = processBatch(batch);
        if (!batch.isEmpty() && lastCompletedOffset >= 0) {
            this.consumer.commit(lastCompletedOffset + 1);
        }
        this.eventLoop.enqueue(POLL_SIGNAL);
    }

    private long processBatch(List<ConsumerRecord> batch) {
        long lastCompletedOffset = -1;
        for (var record : batch) {
            if (!inplaceRetry(record, 0)) {
                return lastCompletedOffset;
            }
            lastCompletedOffset = record.getOffset();
        }
        return lastCompletedOffset;
    }

}
