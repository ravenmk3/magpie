package ravenworks.magpie.engine.source.sample;

import lombok.NonNull;
import ravenworks.magpie.engine.source.SourceConnector;
import ravenworks.magpie.engine.source.SourceProvider;
import ravenworks.magpie.engine.stream.StreamProducer;

import java.util.Map;


/**
 * @author Raven
 */
public class SampleSourceProvider implements SourceProvider {

    @Override
    public String type() {
        return "sample";
    }

    @Override
    public SourceConnector create(@NonNull StreamProducer producer,
                                  @NonNull String name,
                                  @NonNull Map<String, Object> properties) {
        return new SampleSourceConnector(producer, name, properties);
    }

}
