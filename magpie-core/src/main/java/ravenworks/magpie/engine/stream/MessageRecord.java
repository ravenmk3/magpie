package ravenworks.magpie.engine.stream;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;


/**
 * @author Raven
 */
@Data
@Accessors(chain = true)
public class MessageRecord implements Serializable {

    private String id;
    private String type;
    private LocalDateTime time;
    private String tenantId;
    private String topic;
    private String partitionKey;
    private Map<String, String> headers;
    private byte[] payload;

}
