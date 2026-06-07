package ravenworks.magpie.engine.target.print;

import lombok.NonNull;
import ravenworks.magpie.engine.target.TargetConnector;
import ravenworks.magpie.engine.target.TargetDefinition;
import ravenworks.magpie.engine.target.TargetProvider;
import ravenworks.magpie.engine.stream.StreamProvider;
import ravenworks.magpie.engine.stream.StreamRegistry;


/**
 * @author Raven
 */
public class PrintTargetProvider implements TargetProvider {

    private final StreamRegistry streamRegistry;

    public PrintTargetProvider(@NonNull StreamRegistry streamRegistry) {
        this.streamRegistry = streamRegistry;
    }

    @Override
    public String type() {
        return "print";
    }

    @Override
    public TargetConnector create(@NonNull StreamProvider provider,
                                  @NonNull TargetDefinition definition) {
        return new PrintTargetConnector(provider, this.streamRegistry,
                definition.getName(), definition.getTopic());
    }

}
