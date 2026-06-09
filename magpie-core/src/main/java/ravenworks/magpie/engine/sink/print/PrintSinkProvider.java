package ravenworks.magpie.engine.sink.print;

import lombok.NonNull;
import ravenworks.magpie.engine.sink.SinkConnector;
import ravenworks.magpie.engine.sink.SinkProvider;
import ravenworks.magpie.engine.sink.TargetDefinition;
import ravenworks.magpie.engine.stream.OffsetTracker;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;


/**
 * @author Raven
 */
public class PrintSinkProvider implements SinkProvider {

    private final StreamRegistry streamRegistry;
    private final OffsetTracker offsetTracker;

    public PrintSinkProvider(@NonNull StreamRegistry streamRegistry,
                             @NonNull OffsetTracker offsetTracker) {
        this.streamRegistry = streamRegistry;
        this.offsetTracker = offsetTracker;
    }

    @Override
    public String type() {
        return "print";
    }

    @Override
    public SinkConnector create(@NonNull StreamProvider provider,
                                @NonNull TargetDefinition definition) {
        return new PrintSinkConnector(provider, this.streamRegistry, this.offsetTracker,
                definition.getName(), definition.getTopic());
    }

}
