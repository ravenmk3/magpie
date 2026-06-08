package ravenworks.magpie.engine.sink;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.stream.StreamProvider;

import java.util.List;


/**
 * @author Raven
 */
@Slf4j
@RequiredArgsConstructor
public class SinkFactoryImpl implements SinkFactory {

    @NonNull
    private final List<SinkProvider> providers;

    @Override
    public SinkConnector create(@NonNull StreamProvider provider,
                                @NonNull TargetDefinition definition) {
        for (var p : this.providers) {
            if (p.type().equals(definition.getType())) {
                return p.create(provider, definition);
            }
        }
        throw new IllegalArgumentException("Unknown target type: " + definition.getType());
    }

}
