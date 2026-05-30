package ravenworks.magpie.server.config;

import lombok.NonNull;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ravenworks.magpie.engine.rabbitmq.RabbitStreamProvider;
import ravenworks.magpie.engine.stream.StreamProvider;

import java.util.Map;


/**
 * @author Raven
 */
@Configuration
public class StreamProviderConfig {

    @Bean
    private static StreamProvider streamProvider(@NonNull RabbitStreamProperties properties) {
        return new RabbitStreamProvider(properties.getUris());
    }

    @Bean
    public static SmartLifecycle streamProviderLifecycle(StreamProvider provider) {
        return new SmartLifecycle() {

            @Override
            public void start() {
                provider.create("demo", 3, Map.of());
            }

            @Override
            public void stop() {
            }

            @Override
            public boolean isRunning() {
                return false;
            }
        };
    }

}
