package ravenworks.magpie.engine.source;

import ravenworks.magpie.engine.stream.StreamProducer;


/**
 * @author Raven
 */
public interface SourceFactory {

    SourceConnector create(StreamProducer producer, SourceDefinition definition);

}
