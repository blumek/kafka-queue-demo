package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.port.EngineOutcome.Failed;

import java.time.Duration;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

record FaultPolicy(double transientRate,
                   double rateLimitedRate,
                   double malformedRate,
                   double alwaysFailsRate,
                   Duration timeoutAfter,
                   Seeds seeds) {

    private static final long MALFORMED = 1;
    private static final long ALWAYS_FAILS = 2;
    private static final long RATE_LIMITED = 3;
    private static final long TRANSIENT = 4;

    FaultPolicy {
        requireRate(transientRate, "transientRate");
        requireRate(rateLimitedRate, "rateLimitedRate");
        requireRate(malformedRate, "malformedRate");
        requireRate(alwaysFailsRate, "alwaysFailsRate");
        requireNonNull(timeoutAfter);
        requireNonNull(seeds);
        if (timeoutAfter.isNegative() || timeoutAfter.isZero()) {
            throw new IllegalArgumentException("timeoutAfter must be positive: " + timeoutAfter);
        }
    }

    static FaultPolicy defaults(final Seeds seeds) {
        return new FaultPolicy(0.02, 0.01, 0.005, 0.002, Duration.ofSeconds(30), seeds);
    }

    Optional<Failed> evaluate(final long seed, final int attempt, final InferenceRequest request) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be positive: " + attempt);
        }
        if (draws(malformedRate, seed, MALFORMED)) {
            return Optional.of(new Failed.MalformedInput("prompt rejected by tokenizer"));
        }
        if (draws(alwaysFailsRate, seed, ALWAYS_FAILS)) {
            return Optional.of(new Failed.Unavailable("model " + request.model().value() + " rejects job " + request.jobId().value()));
        }
        if (draws(rateLimitedRate, seed, RATE_LIMITED, attempt)) {
            return Optional.of(new Failed.RateLimited(Optional.of(Duration.ofMillis(250L * attempt))));
        }
        if (draws(transientRate, seed, TRANSIENT, attempt)) {
            return Optional.of(new Failed.Timeout(timeoutAfter));
        }
        return Optional.empty();
    }

    private boolean draws(final double rate, final long... parts) {
        return seeds.nextDouble(parts) < rate;
    }

    private static void requireRate(final double rate, final String name) {
        if (!(rate >= 0.0 && rate <= 1.0)) {
            throw new IllegalArgumentException(name + " must be within [0, 1]: " + rate);
        }
    }
}
