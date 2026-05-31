package ravenworks.magpie.engine.source;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;


/**
 * @author Raven
 */
@Data
public class SourceDefinition implements Serializable {

    private String name;
    private String type;
    private Map<String, Object> properties;

}
