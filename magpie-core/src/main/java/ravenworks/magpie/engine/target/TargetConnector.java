package ravenworks.magpie.engine.target;

import ravenworks.magpie.common.runtime.Lifecycle;


/**
 * @author Raven
 */
public interface TargetConnector extends Lifecycle {

    String type();

    String name();

}
