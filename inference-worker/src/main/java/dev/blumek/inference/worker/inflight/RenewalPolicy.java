package dev.blumek.inference.worker.inflight;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

public record RenewalPolicy(Duration maxProcessingTime, double lockFraction) {
    public static final double DEFAULT_LOCK_FRACTION = 0.5;

    public RenewalPolicy {
        requireNonNull(maxProcessingTime);
        rejectAnEmptyProcessingBudget(maxProcessingTime);
        rejectAFractionOutsideTheLock(lockFraction);
    }

    public static RenewalPolicy over(final Duration maxProcessingTime) {
        return new RenewalPolicy(maxProcessingTime, DEFAULT_LOCK_FRACTION);
    }

    public RenewalSchedule against(final Duration lockTimeout) {
        final var renewAfter = fractionOf(lockTimeout);
        return new RenewalSchedule(renewAfter, renewalsWithinTheBudget(renewAfter));
    }

    private Duration fractionOf(final Duration lockTimeout) {
        return Duration.ofNanos(Math.max(1L, Math.round(lockTimeout.toNanos() * lockFraction)));
    }

    private int renewalsWithinTheBudget(final Duration renewAfter) {
        return (int) Math.min(Integer.MAX_VALUE, maxProcessingTime.toNanos() / renewAfter.toNanos());
    }

    private static void rejectAnEmptyProcessingBudget(final Duration maxProcessingTime) {
        if (maxProcessingTime.isNegative() || maxProcessingTime.isZero()) {
            throw new IllegalArgumentException("A job needs a processing budget of its own, not " + maxProcessingTime);
        }
    }

    private static void rejectAFractionOutsideTheLock(final double lockFraction) {
        if (lockFraction <= 0 || lockFraction >= 1) {
            throw new IllegalArgumentException(
                    "Renewing after %s of the lock leaves no margin for a late poll".formatted(lockFraction));
        }
    }
}
