package ravenworks.magpie.engine.stream;

import java.time.Duration;
import java.util.List;


/**
 * @author Raven
 */
public interface StreamConsumer extends AutoCloseable {

    int partition();

    void consume(int bufferSize);

    List<ConsumerRecord> poll(int count, Duration timeout);

    void commit(long offset);

}
