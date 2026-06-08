package ravenworks.magpie.engine.sink;

import ravenworks.magpie.engine.stream.StreamProvider;


/**
 * @author Raven
 */
public interface SinkProvider {

    String type();

    SinkConnector create(StreamProvider provider, TargetDefinition definition);

}
