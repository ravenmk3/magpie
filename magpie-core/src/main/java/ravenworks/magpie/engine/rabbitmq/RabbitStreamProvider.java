package ravenworks.magpie.engine.rabbitmq;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.StreamCreator;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import ravenworks.magpie.engine.stream.StreamDefinition;
import ravenworks.magpie.engine.stream.StreamProducer;
import ravenworks.magpie.engine.stream.StreamProvider;

import java.util.List;
import java.util.Map;


/**
 * @author Raven
 */
@Slf4j
public class RabbitStreamProvider implements StreamProvider {

    private final Environment environment;

    public RabbitStreamProvider(@NonNull List<String> uris) {
        this.environment = Environment.builder()
                .id("magpie")
                .uris(uris)
                .build();
    }

    @Override
    public void create(@NonNull StreamDefinition definition) {
        log.info("Creating stream {}", definition);
        for (int i = 0; i < definition.partitions(); i++) {
            var partitionName = RabbitUtils.streamQueueName(definition.name(), i);
            this.createStream(partitionName, definition.properties());
        }
    }

    @Override
    public StreamProducer producer(@NonNull StreamDefinition definition) {
        return new RabbitStreamProducer(this.environment, definition);
    }

    private void createStream(@NonNull String name,
                              @NonNull Map<String, Object> arguments) {
        var creator = this.environment.streamCreator()
                .stream(name);
        arguments.forEach((k, v) -> creator.argument(k, String.valueOf(v)));
        creator.leaderLocator(StreamCreator.LeaderLocator.BALANCED)
                .create();
    }

    @Override
    public void close() {
        log.info("Closing RabbitMQ stream environment");
        this.environment.close();
        log.info("Closed RabbitMQ stream environment");
    }

}
