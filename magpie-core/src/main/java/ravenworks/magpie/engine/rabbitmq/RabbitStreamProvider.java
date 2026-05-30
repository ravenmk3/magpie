package ravenworks.magpie.engine.rabbitmq;

import com.rabbitmq.stream.Environment;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import ravenworks.magpie.engine.model.StreamDefinition;
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
            var partitionName = String.format("magpie-stream.%s-%d", definition.name(), i);
            this.createStream(partitionName, definition.properties());
        }
    }

    private void createStream(@NonNull String name,
                              @NonNull Map<String, Object> arguments) {
        var creator = this.environment.streamCreator()
                .stream(name);
        arguments.forEach((k, v) -> creator.argument(k, String.valueOf(v)));
        creator.create();
    }

    @Override
    public void close() {
        log.info("Closing RabbitMQ stream environment");
        this.environment.close();
        log.info("Closed RabbitMQ stream environment");
    }

}
