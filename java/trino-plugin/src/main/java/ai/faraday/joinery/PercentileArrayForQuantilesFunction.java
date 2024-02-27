package ai.faraday.joinery;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import static io.trino.spi.type.DoubleType.DOUBLE;

/**
 * Internal helper which generates an array of percentiles from 0 to 1 for use
 * in emulating `APPROX_QUANTILES`.
 */
public class PercentileArrayForQuantilesFunction {
    @ScalarFunction("percentile_array_for_quantiles")
    @Description("(Internal helper) Returns an array of quantiles as numbers from 0 to 1")
    @SqlNullable
    @SqlType("array(double)")
    public static Block percentileArrayForQuantiles(
            @SqlNullable @SqlType(StandardTypes.BIGINT) Long quantiles) {
        if (quantiles == null) {
            return null;
        }
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, 1 + quantiles.intValue());
        for (int i = 0; i < quantiles; i++) {
            DOUBLE.writeDouble(builder, (double) i / quantiles);
        }
        DOUBLE.writeDouble(builder, 1.0);
        return builder.build();
    }
}
