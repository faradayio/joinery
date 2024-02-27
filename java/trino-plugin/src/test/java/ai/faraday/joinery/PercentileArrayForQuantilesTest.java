package ai.faraday.joinery;

import static org.junit.Assert.assertEquals;
import static io.trino.spi.type.DoubleType.DOUBLE;

import org.junit.Test;

import io.trino.spi.block.Block;

/**
 * Unit test for simple App.
 */
public class PercentileArrayForQuantilesTest {
    @Test
    public void handlesNull() {
        assertEquals(
                PercentileArrayForQuantilesFunction.percentileArrayForQuantiles(null),
                null);
    }

    @Test
    public void calculatesQuartiles() {
        Block block = PercentileArrayForQuantilesFunction.percentileArrayForQuantiles(4L);
        assertEquals(block.getPositionCount(), 5);
        assertEquals(DOUBLE.getDouble(block, 0), 0.0, 0.001);
        assertEquals(DOUBLE.getDouble(block, 1), 0.25, 0.001);
        assertEquals(DOUBLE.getDouble(block, 2), 0.5, 0.001);
        assertEquals(DOUBLE.getDouble(block, 3), 0.75, 0.001);
        assertEquals(DOUBLE.getDouble(block, 4), 1.0, 0.001);
    }
}
