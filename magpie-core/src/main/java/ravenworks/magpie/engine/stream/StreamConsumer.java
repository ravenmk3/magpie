package ravenworks.magpie.engine.stream;

import java.time.Duration;
import java.util.List;


/**
 * @author Raven
 */
public interface StreamConsumer {

    int partition();

    void start();

    void stop();

    List<ConsumerRecord> poll(int count, Duration timeout);

    void commit(long offset);

}
