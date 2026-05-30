package ravenworks.magpie.engine.stream;

import java.util.concurrent.CompletableFuture;


/**
 * @author Raven
 */
public interface StreamProducer extends AutoCloseable {

    CompletableFuture<SendResult> send(MessageRecord record);

}
