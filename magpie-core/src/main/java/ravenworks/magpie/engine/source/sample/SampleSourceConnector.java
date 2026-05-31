package ravenworks.magpie.engine.source.sample;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.runtime.EventLoop;
import ravenworks.magpie.common.util.Uuids;
import ravenworks.magpie.engine.source.SourceConnector;
import ravenworks.magpie.engine.stream.MessageRecord;
import ravenworks.magpie.engine.stream.StreamProducer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
@Slf4j
public class SampleSourceConnector implements SourceConnector {

    private static final int DEFAULT_IDLE_TIMEOUT_MS = 5_000;
    private static final int DEFAULT_BATCH_SIZE = 10;

    private final String name;
    private final String topic;
    private final StreamProducer producer;
    private final int batchSize;
    private final EventLoop eventLoop;

    public SampleSourceConnector(@NonNull StreamProducer producer,
                                 @NonNull String name,
                                 @NonNull Map<String, Object> properties) {
        this.name = name;
        this.topic = getStringProperty(properties, "topic", name);
        this.producer = producer;
        this.batchSize = getIntProperty(properties, "batchSize", DEFAULT_BATCH_SIZE);
        int idleTimeout = getIntProperty(properties, "idleTimeout", DEFAULT_IDLE_TIMEOUT_MS);
        this.eventLoop = new EventLoop("src-" + name, idleTimeout, this::dispatch);
    }

    @Override
    public String type() {
        return "sample";
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public void start() {
        this.eventLoop.start();
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return this.eventLoop.shutdown();
    }

    private void dispatch(Object event) {
        if (event instanceof EventLoop.Idle) {
            this.sendMessages();
        }
    }

    private void sendMessages() {
        for (int i = 0; i < this.batchSize; i++) {
            var msg = new MessageRecord()
                    .setId(Uuids.uuid7Hex())
                    .setTenantId("sample")
                    .setTopic(this.topic)
                    .setPartitionKey(Uuids.uuid7Hex())
                    .setPayload(("Sample message at " + Instant.now()).getBytes(StandardCharsets.UTF_8));
            this.producer.send(msg);
        }
    }

    private static int getIntProperty(Map<String, Object> properties, String key, int defaultValue) {
        Object value = properties.get(key);
        if (value instanceof Number n) {
            return n.intValue();
        }
        return defaultValue;
    }

    private static String getStringProperty(Map<String, Object> properties, String key, String defaultValue) {
        Object value = properties.get(key);
        if (value instanceof String s) {
            return s;
        }
        return defaultValue;
    }

}
