package ravenworks.magpie.engine.retry;

import ravenworks.magpie.engine.stream.ConsumerRecord;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

public interface RetryMessageStore {

    Set<String> listKeys(String consumer);

    List<RetryRecord> list(String consumer, int count);

    List<RetryRecord> listRetryable(String consumer, int count);

    void save(String consumer, ConsumerRecord record);

    void succeeded(String id);

    void failed(String id, LocalDateTime retryAt);

}
