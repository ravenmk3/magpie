package ravenworks.magpie.engine.stream;

/**
 * @author Raven
 */
public interface MessageHandler {

    void handle(ConsumerRecord record);

}
