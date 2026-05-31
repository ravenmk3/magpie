package ravenworks.magpie.engine.stream;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.Map;


/**
 * @author Raven
 */
@Data
@Accessors(chain = true)
public class MessageRecord implements Serializable {

    private String id;
    private String tenantId;
    private String partitionKey;
    private String topic;
    private Map<String, String> headers;
    private byte[] payload;

}
