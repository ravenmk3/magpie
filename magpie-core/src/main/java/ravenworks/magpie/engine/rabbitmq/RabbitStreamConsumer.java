package ravenworks.magpie.engine.rabbitmq;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.common.util.TimeUtils;
import ravenworks.magpie.engine.stream.ConsumerRecord;
import ravenworks.magpie.engine.stream.MessageHandler;
import ravenworks.magpie.engine.stream.StreamConsumer;
import ravenworks.magpie.engine.stream.StreamDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Raven
 */
@Slf4j
public class RabbitStreamConsumer implements StreamConsumer {

    private static final Set<String> KNOWN_KEYS = Set.of("x-partition-key", "x-tenant-id", "x-type", "x-time");

    private final Environment environment;
    private final StreamDefinition definition;
    private final int partition;
    private final String topic;
    private final AtomicBoolean consuming = new AtomicBoolean(false);

    private final String name;

    private volatile Consumer rmqConsumer;

    public RabbitStreamConsumer(@NonNull Environment environment,
                                @NonNull StreamDefinition definition,
                                int partition,
                                String name) {
        this.environment = environment;
        this.definition = definition;
        this.partition = partition;
        this.name = name;
        this.topic = definition.name();
    }

    @Override
    public int partition() {
        return this.partition;
    }

    @Override
    public void consume(long offset, @NonNull MessageHandler handler) {
        if (!this.consuming.compareAndSet(false, true)) {
            throw new IllegalStateException("Already consuming");
        }
        OffsetSpecification offsetSpec;
        if (offset > 0) {
            offsetSpec = OffsetSpecification.offset(offset);
        } else if (offset == -1) {
            offsetSpec = OffsetSpecification.next();
        } else {
            offsetSpec = OffsetSpecification.first();
        }
        String streamName = RabbitUtils.streamQueueName(this.definition.name(), this.partition);
        this.rmqConsumer = this.environment.consumerBuilder()
                .stream(streamName)
                .name("magpie-" + this.name + "-" + this.partition)
                .offset(offsetSpec)
                .messageHandler((ctx, msg) -> {
                    ConsumerRecord record = convert(msg, ctx.offset());
                    handler.handle(record);
                })
                .build();
    }

    @Override
    public void close() {
        if (this.rmqConsumer != null) {
            this.rmqConsumer.close();
            this.rmqConsumer = null;
        }
    }

    private ConsumerRecord convert(@NonNull Message msg, long offset) {
        var record = new ConsumerRecord();
        record.setOffset(offset);
        record.setPayload(msg.getBodyAsBinary());
        record.setTopic(this.topic);

        if (msg.getProperties() != null) {
            Object mid = msg.getProperties().getMessageId();
            if (mid != null) {
                record.setId(mid.toString());
            }
        }
        Map<String, Object> appProps = msg.getApplicationProperties();
        if (appProps != null) {
            record.setType((String) appProps.get("x-type"));
            record.setTenantId((String) appProps.get("x-tenant-id"));
            record.setPartitionKey((String) appProps.get("x-partition-key"));
            String timeStr = (String) appProps.get("x-time");
            if (timeStr != null) {
                record.setTime(TimeUtils.parseRfc3339(timeStr));
            }
            Map<String, String> headers = new HashMap<>();
            appProps.forEach((k, v) -> {
                if (!KNOWN_KEYS.contains(k) && v instanceof String s) {
                    headers.put(k, s);
                }
            });
            record.setHeaders(headers);
        }
        return record;
    }

}
