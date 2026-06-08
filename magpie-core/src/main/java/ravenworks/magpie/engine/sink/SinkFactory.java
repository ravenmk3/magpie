package ravenworks.magpie.engine.sink;

import ravenworks.magpie.engine.stream.StreamProvider;


/**
 * @author Raven
 */
public interface SinkFactory {

    SinkConnector create(StreamProvider provider, TargetDefinition definition);

}
