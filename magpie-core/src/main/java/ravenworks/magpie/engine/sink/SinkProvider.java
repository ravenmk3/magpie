package ravenworks.magpie.engine.sink;

import java.util.Map;


/**
 * @author Raven
 */
public interface SinkProvider {

    String type();

    SinkConnector create(String name, Map<String, Object> properties);

}
