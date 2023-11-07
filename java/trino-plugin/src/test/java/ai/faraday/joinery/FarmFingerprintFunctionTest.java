package ai.faraday.joinery;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/**
 * Unit test for simple App.
 */
public class FarmFingerprintFunctionTest {
    @Test
    public void handlesNull() {
        assertEquals(FarmFingerprintFunction.farmFingerprint(null), null);
    }

    @Test
    public void calculatesExpectedHash() {
        Slice hello = Slices.copiedBuffer("Hello", UTF_8);
        Long expected = -3042045079152025465L;
        assertEquals(FarmFingerprintFunction.farmFingerprint(hello), expected);
    }
}
