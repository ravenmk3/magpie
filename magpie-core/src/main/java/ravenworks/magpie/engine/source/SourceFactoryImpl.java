package ravenworks.magpie.engine.source;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ravenworks.magpie.engine.stream.StreamProducer;

import java.util.List;


/**
 * @author Raven
 */
@Slf4j
@RequiredArgsConstructor
public class SourceFactoryImpl implements SourceFactory {

    @NonNull
    private final List<SourceProvider> providers;

    @Override
    public SourceConnector create(@NonNull StreamProducer producer,
                                  @NonNull SourceDefinition definition) {
        for (var provider : this.providers) {
            if (provider.type().equals(definition.getType())) {
                return provider.create(producer, definition.getName(), definition.getProperties());
            }
        }
        throw new IllegalArgumentException("Unknown source type: " + definition.getType());
    }

}
