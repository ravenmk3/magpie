package ravenworks.magpie.engine.sink.http;

import lombok.NonNull;
import ravenworks.magpie.engine.sink.SinkConnector;
import ravenworks.magpie.engine.sink.SinkProvider;
import ravenworks.magpie.engine.sink.TargetDefinition;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;


/**
 * @author Raven
 */
public class HttpSinkProvider implements SinkProvider {

    private final StreamRegistry streamRegistry;

    public HttpSinkProvider(@NonNull StreamRegistry streamRegistry) {
        this.streamRegistry = streamRegistry;
    }

    @Override
    public String type() {
        return "http";
    }

    @Override
    public SinkConnector create(@NonNull StreamProvider provider,
                                @NonNull TargetDefinition definition) {
        return new HttpSinkConnector(provider, this.streamRegistry,
                definition.getName(), definition.getTopic(), definition.getProperties());
    }

}
