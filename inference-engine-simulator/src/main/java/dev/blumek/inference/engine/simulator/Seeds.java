package dev.blumek.inference.engine.simulator;

import java.util.Arrays;
import java.util.SplittableRandom;

final class Seeds {

    double nextDouble(final long... parts) {
        return random(parts).nextDouble();
    }

    double nextGaussian(final long... parts) {
        return random(parts).nextGaussian();
    }

    private static SplittableRandom random(final long... parts) {
        return new SplittableRandom(Arrays.hashCode(parts));
    }
}
