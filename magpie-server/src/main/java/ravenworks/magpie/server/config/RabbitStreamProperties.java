package ravenworks.magpie.server.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;
import java.util.List;


/**
 * @author Raven
 */
@Data
@ConfigurationProperties("magpie.rabbitmq-stream")
public class RabbitStreamProperties implements Serializable {

    private List<String> uris = List.of("rabbitmq-stream://guest:guest@localhost:5552/%2f");

}
