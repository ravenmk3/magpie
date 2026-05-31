package ravenworks.magpie.engine.stream;


/**
 * @author Raven
 */
public interface StreamProvider extends AutoCloseable {

    void create(StreamDefinition definition);

    StreamProducer producer(StreamDefinition definition);

}
