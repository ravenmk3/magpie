package ravenworks.magpie.engine.store;

import ravenworks.magpie.engine.model.TopicDefinition;

import java.util.List;


/**
 * @author Raven
 */
public interface MetaStore {

    List<TopicDefinition> getTopics();

}
