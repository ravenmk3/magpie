package ravenworks.magpie.engine.source;

import ravenworks.magpie.engine.stream.StreamProducer;

import java.util.Map;


/**
 * @author Raven
 */
public interface SourceProvider {

    String type();

    SourceConnector create(StreamProducer producer, String name, Map<String, Object> properties);

}
