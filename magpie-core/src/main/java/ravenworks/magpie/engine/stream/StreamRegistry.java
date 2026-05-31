package ravenworks.magpie.engine.stream;


import java.util.List;


/**
 * @author Raven
 */
public interface StreamRegistry {

    List<StreamDefinition> getStreams();

    StreamDefinition getStream(String name);

}
