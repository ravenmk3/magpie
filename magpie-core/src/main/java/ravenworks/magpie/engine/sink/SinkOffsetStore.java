package ravenworks.magpie.engine.sink;

import lombok.NonNull;
import org.springframework.transaction.annotation.Transactional;
import ravenworks.magpie.domain.entity.SinkOffsetEntity;
import ravenworks.magpie.domain.repository.SinkOffsetRepository;

public class SinkOffsetStore {

    private final SinkOffsetRepository repository;

    public SinkOffsetStore(@NonNull SinkOffsetRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public long read(@NonNull String target, int partition) {
        String id = target + ":" + partition;
        var existing = this.repository.findById(id);
        if (existing.isPresent()) {
            return existing.get().getOffset();
        }
        this.repository.save(new SinkOffsetEntity(id, target, partition, 0L));
        return 0L;
    }

    @Transactional
    public void write(@NonNull String target, int partition, long offset) {
        String id = target + ":" + partition;
        int rows = this.repository.updateOffset(id, offset);
        if (rows == 0) {
            throw new IllegalStateException(
                    "Sink offset not found for target=" + target + " partition=" + partition);
        }
    }

}
