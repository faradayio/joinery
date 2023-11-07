package ai.faraday.joinery;

import java.nio.charset.StandardCharsets;

import com.google.common.hash.Hashing;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

public class FarmFingerprintFunction {
    @ScalarFunction("farm_fingerprint")
    @Description("Returns FARM_FINGERPRINT of the given string")
    @SqlNullable
    @SqlType(StandardTypes.BIGINT)
    public static Long farmFingerprint(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice string) {
        if (string == null) {
            return null;
        }
        return Hashing.farmHashFingerprint64().hashString(string.toStringUtf8(), StandardCharsets.UTF_8).asLong();
    }
}
