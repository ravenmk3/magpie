package ravenworks.magpie.engine.target;

import ravenworks.magpie.engine.stream.StreamProvider;


/**
 * @author Raven
 */
public interface TargetFactory {

    TargetConnector create(StreamProvider provider, TargetDefinition definition);

}
