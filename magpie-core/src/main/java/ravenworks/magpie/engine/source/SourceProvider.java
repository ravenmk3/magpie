package ravenworks.magpie.engine.source;

import java.util.Map;


/**
 * @author Raven
 */
public interface SourceProvider {

    String type();

    SourceConnector create(String name, Map<String, Object> properties);

}
