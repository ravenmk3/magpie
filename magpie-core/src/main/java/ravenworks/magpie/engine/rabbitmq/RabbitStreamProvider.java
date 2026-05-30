package ravenworks.magpie.engine.rabbitmq;

import com.rabbitmq.stream.Environment;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
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
    public void create(@NonNull String name,
                       int partitions,
                       @NonNull Map<String, String> properties) {
        log.info("Creating stream {} with {} partitions", name, partitions);
        for (int i = 0; i < partitions; i++) {
            var partitionName = String.format("magpie-stream.%s-%d", name, i);
            this.createStream(partitionName, properties);
        }
    }

    private void createStream(@NonNull String name,
                              @NonNull Map<String, String> arguments) {
        var creator = this.environment.streamCreator()
                .stream(name);
        arguments.forEach(creator::argument);
        creator.create();
    }

    @Override
    public void close() {
        log.info("Closing RabbitMQ stream environment");
        this.environment.close();
        log.info("Closed RabbitMQ stream environment");
    }

}
