package ravenworks.magpie.engine.stream;

import lombok.Data;
import lombok.experimental.Accessors;


/**
 * @author Raven
 */
@Data
@Accessors(chain = true)
public class SendResult {

    boolean succeeded;
    String error;
    MessageRecord message;

}
