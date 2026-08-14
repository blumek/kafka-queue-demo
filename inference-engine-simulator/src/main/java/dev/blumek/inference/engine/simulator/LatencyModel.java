package dev.blumek.inference.engine.simulator;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

record LatencyModel(Duration overhead,
                    Duration perToken,
                    double sigma,
                    double minCompletionFraction,
                    double maxCompletionFraction,
                    Seeds seeds) {

    private static final long TOKENS = 11;
    private static final long LATENCY = 12;
    private static final double MAX_SIGMAS = 3.0;

    LatencyModel {
        requireNonNull(overhead);
        requireNonNull(perToken);
        requireNonNull(seeds);
        if (overhead.isNegative()) {
            throw new IllegalArgumentException("overhead must not be negative: " + overhead);
        }
        if (perToken.isNegative() || perToken.isZero()) {
            throw new IllegalArgumentException("perToken must be positive: " + perToken);
        }
        if (!(sigma > 0.0)) {
            throw new IllegalArgumentException("sigma must be positive: " + sigma);
        }
        if (!(minCompletionFraction > 0.0 && minCompletionFraction <= maxCompletionFraction && maxCompletionFraction <= 1.0)) {
            throw new IllegalArgumentException(
                    "completion fractions must satisfy 0 < min <= max <= 1: " + minCompletionFraction + ", " + maxCompletionFraction);
        }
    }

    static LatencyModel defaults(final Seeds seeds) {
        return new LatencyModel(Duration.ofMillis(120), Duration.ofMillis(18), 0.35, 0.15, 1.0, seeds);
    }

    int drawCompletionTokens(final long seed, final int maxTokens) {
        if (maxTokens < 1) {
            throw new IllegalArgumentException("maxTokens must be positive: " + maxTokens);
        }
        final var spread = maxCompletionFraction - minCompletionFraction;
        final var fraction = minCompletionFraction + seeds.nextDouble(seed, TOKENS) * spread;
        return Math.clamp(Math.round(maxTokens * fraction), 1, maxTokens);
    }

    Duration draw(final long seed, final int completionTokens) {
        if (completionTokens < 1) {
            throw new IllegalArgumentException("completionTokens must be positive: " + completionTokens);
        }
        final var gaussian = Math.clamp(seeds.nextGaussian(seed, LATENCY), -MAX_SIGMAS, MAX_SIGMAS);
        final var generation = Math.round(perToken.toNanos() * (double) completionTokens * Math.exp(sigma * gaussian));
        return overhead.plusNanos(generation);
    }
}
