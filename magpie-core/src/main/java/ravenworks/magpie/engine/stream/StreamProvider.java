package ravenworks.magpie.engine.stream;

import java.util.Map;


/**
 * @author Raven
 */
public interface StreamProvider extends AutoCloseable {

    void create(String name,
                int partitions,
                Map<String, Object> properties);

}
