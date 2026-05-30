package ravenworks.magpie.engine.model;

import java.util.Map;


/**
 * @author Raven
 */
public record TopicDefinition(String name,
                              int partitions,
                              Map<String, Object> properties) {

}
