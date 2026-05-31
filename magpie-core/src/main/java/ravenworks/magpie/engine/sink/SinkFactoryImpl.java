package ravenworks.magpie.engine.sink;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
    public SinkConnector create(@NonNull SinkDefinition definition) {
        for (var provider : this.providers) {
            if (provider.type().equals(definition.getType())) {
                return provider.create(definition.getName(), definition.getProperties());
            }
        }
        throw new IllegalArgumentException("Unknown sink type: " + definition.getType());
    }

}
