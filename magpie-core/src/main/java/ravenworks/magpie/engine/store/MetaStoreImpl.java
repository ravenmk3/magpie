package ravenworks.magpie.engine.store;

import lombok.NonNull;
import ravenworks.magpie.domain.entity.TopicEntity;
import ravenworks.magpie.domain.repository.TopicRepository;
import ravenworks.magpie.engine.model.TopicDefinition;

import java.util.List;


/**
 * @author Raven
 */
public class MetaStoreImpl implements MetaStore {

    private final TopicRepository topicRepository;

    public MetaStoreImpl(@NonNull TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    @Override
    public List<TopicDefinition> getTopics() {
        return this.topicRepository.findAll()
                .stream()
                .map(this::toDefinition)
                .toList();
    }

    private TopicDefinition toDefinition(TopicEntity entity) {
        return new TopicDefinition(
                entity.getName(),
                entity.getPartitions(),
                entity.getProperties()
        );
    }

}
