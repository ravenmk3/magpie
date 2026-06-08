package ravenworks.magpie.engine.sink.print;

import lombok.NonNull;
import ravenworks.magpie.engine.sink.SinkConnector;
import ravenworks.magpie.engine.sink.SinkOffsetStore;
import ravenworks.magpie.engine.sink.SinkProvider;
import ravenworks.magpie.engine.sink.TargetDefinition;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;


/**
 * @author Raven
 */
public class PrintSinkProvider implements SinkProvider {

    private final StreamRegistry streamRegistry;
    private final SinkOffsetStore offsetStore;

    public PrintSinkProvider(@NonNull StreamRegistry streamRegistry,
                             @NonNull SinkOffsetStore offsetStore) {
        this.streamRegistry = streamRegistry;
        this.offsetStore = offsetStore;
    }

    @Override
    public String type() {
        return "print";
    }

    @Override
    public SinkConnector create(@NonNull StreamProvider provider,
                                @NonNull TargetDefinition definition) {
        return new PrintSinkConnector(provider, this.streamRegistry, this.offsetStore,
                definition.getName(), definition.getTopic());
    }

}
