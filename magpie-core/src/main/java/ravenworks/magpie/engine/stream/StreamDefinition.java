package ravenworks.magpie.engine.stream;

import java.util.Map;


/**
 * @author Raven
 */
public record StreamDefinition(String name,
                               int partitions,
                               Map<String, Object> properties) {

}
