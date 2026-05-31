package ravenworks.magpie.engine.stream;

/**
 * @author Raven
 */
public interface StreamConsumer extends AutoCloseable {

    int partition();

    void consume(long offset, MessageHandler handler);

}
