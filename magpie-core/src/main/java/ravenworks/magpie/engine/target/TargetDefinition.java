package ravenworks.magpie.engine.target;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;


/**
 * @author Raven
 */
@Data
public class TargetDefinition implements Serializable {

    private String name;
    private String type;
    private String topic;
    private Map<String, Object> properties;

}
