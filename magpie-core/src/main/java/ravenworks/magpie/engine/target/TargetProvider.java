package ravenworks.magpie.engine.target;

import ravenworks.magpie.engine.stream.StreamProvider;


/**
 * @author Raven
 */
public interface TargetProvider {

    String type();

    TargetConnector create(StreamProvider provider, TargetDefinition definition);

}
