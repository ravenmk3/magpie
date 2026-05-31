package ravenworks.magpie.engine.source;


/**
 * @author Raven
 */
public interface SourceFactory {

    SourceConnector create(SourceDefinition definition);

}
