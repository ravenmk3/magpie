package ravenworks.magpie.engine.sink;

import lombok.NonNull;
import ravenworks.magpie.domain.entity.EventSinkEntity;
import ravenworks.magpie.domain.repository.EventSinkRepository;

import java.util.List;


/**
 * @author Raven
 */
public class SinkRegistryImpl implements SinkRegistry {

    private final EventSinkRepository sinkRepository;

    public SinkRegistryImpl(@NonNull EventSinkRepository sinkRepository) {
        this.sinkRepository = sinkRepository;
    }

    @Override
    public List<SinkDefinition> getSinks() {
        return this.sinkRepository.findAll()
                .stream()
                .map(this::toDefinition)
                .toList();
    }

    private SinkDefinition toDefinition(EventSinkEntity entity) {
        var def = new SinkDefinition();
        def.setName(entity.getName());
        def.setType(entity.getType());
        def.setTopic(entity.getTopic());
        def.setProperties(entity.getProperties());
        return def;
    }

}
