package ravenworks.magpie.server.config;

import lombok.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ravenworks.magpie.engine.rabbitmq.RabbitStreamProvider;
import ravenworks.magpie.engine.stream.StreamProvider;


/**
 * @author Raven
 */
@Configuration
public class StreamProviderConfig {

    @Bean
    private static StreamProvider streamProvider(@NonNull RabbitStreamProperties properties) {
        return new RabbitStreamProvider(properties.getUris());
    }

}
