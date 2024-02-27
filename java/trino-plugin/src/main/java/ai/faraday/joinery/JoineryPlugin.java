package ai.faraday.joinery;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

public class JoineryPlugin implements io.trino.spi.Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return ImmutableSet.<Class<?>>builder()
                .add(FarmFingerprintFunction.class)
                .add(PercentileArrayForQuantilesFunction.class)
                .build();
    }
}
