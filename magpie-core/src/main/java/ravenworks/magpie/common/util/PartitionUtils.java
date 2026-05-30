package ravenworks.magpie.common.util;

import com.google.common.hash.Hashing;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.nio.charset.StandardCharsets;


/**
 * @author Raven
 */
@UtilityClass
public class PartitionUtils {

    public static int partition(@NonNull String key, int partitions) {
        if (key.isEmpty()) {
            return 0;
        }
        return Math.abs(Hashing.murmur3_32_fixed()
                .hashString(key, StandardCharsets.UTF_8)
                .asInt()) % partitions;
    }

}
