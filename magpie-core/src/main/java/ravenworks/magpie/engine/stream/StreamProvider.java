package ravenworks.magpie.engine.stream;

import ravenworks.magpie.engine.model.StreamDefinition;


/**
 * @author Raven
 */
public interface StreamProvider extends AutoCloseable {

    void create(StreamDefinition definition);

    StreamProducer producer(StreamDefinition definition);

}
