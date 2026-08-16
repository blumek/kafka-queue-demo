package dev.blumek.inference.worker.inflight;

import java.time.Duration;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public record RenewalSchedule(Duration renewAfter, int maxRenewals) {

    public RenewalSchedule {
        requireNonNull(renewAfter);
    }

    public Instant dueAt(final InFlight inFlight) {
        return inFlight.lockRefreshedAt().plus(renewAfter);
    }

    public boolean isExhausted(final InFlight inFlight) {
        return inFlight.renewals() >= maxRenewals;
    }
}
