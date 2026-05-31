package ravenworks.magpie.engine.sink;


/**
 * @author Raven
 */
public interface SinkFactory {

    SinkConnector create(SinkDefinition definition);

}
