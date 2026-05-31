package ravenworks.magpie.engine.source;

import lombok.NonNull;
import ravenworks.magpie.domain.entity.EventSourceEntity;
import ravenworks.magpie.domain.repository.EventSourceRepository;

import java.util.List;


/**
 * @author Raven
 */
public class SourceRegistryImpl implements SourceRegistry {

    private final EventSourceRepository sourceRepository;

    public SourceRegistryImpl(@NonNull EventSourceRepository sourceRepository) {
        this.sourceRepository = sourceRepository;
    }

    @Override
    public List<SourceDefinition> getSources() {
        return this.sourceRepository.findAll()
                .stream()
                .map(this::toDefinition)
                .toList();
    }

    private SourceDefinition toDefinition(EventSourceEntity entity) {
        var def = new SourceDefinition();
        def.setName(entity.getName());
        def.setType(entity.getType());
        def.setProperties(entity.getProperties());
        return def;
    }

}
