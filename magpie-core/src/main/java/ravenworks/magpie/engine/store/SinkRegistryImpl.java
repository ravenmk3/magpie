package ravenworks.magpie.engine.store;

import lombok.NonNull;
import ravenworks.magpie.domain.entity.SinkEntity;
import ravenworks.magpie.domain.repository.SinkRepository;
import ravenworks.magpie.engine.sink.SinkDefinition;
import ravenworks.magpie.engine.sink.SinkRegistry;

import java.util.List;


/**
 * @author Raven
 */
public class SinkRegistryImpl implements SinkRegistry {

    private final SinkRepository sinkRepository;

    public SinkRegistryImpl(@NonNull SinkRepository sinkRepository) {
        this.sinkRepository = sinkRepository;
    }

    @Override
    public List<SinkDefinition> getSinks() {
        return this.sinkRepository.findAll()
                .stream()
                .map(this::toDefinition)
                .toList();
    }

    private SinkDefinition toDefinition(SinkEntity entity) {
        var def = new SinkDefinition();
        def.setName(entity.getName());
        def.setType(entity.getType());
        def.setProperties(entity.getProperties());
        return def;
    }

}
