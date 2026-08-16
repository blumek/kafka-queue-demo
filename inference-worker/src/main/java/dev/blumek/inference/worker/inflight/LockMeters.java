package dev.blumek.inference.worker.inflight;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;

public final class LockMeters {
    public static final String RENEWED = "inference.locks.renewed";
    public static final String EXPIRED = "inference.locks.expired";
    public static final String RENEWALS_PER_JOB = "inference.job.renewals";
    public static final String LATE_COMPLETIONS = "inference.job.completions.late";
    public static final String REASON = "reason";

    private final MeterRegistry registry;
    private final Counter renewed;
    private final DistributionSummary renewalsPerJob;
    private final Counter lateCompletions;

    public LockMeters(final MeterRegistry registry) {
        this.registry = registry;
        this.renewed = Counter.builder(RENEWED)
                .description("Acquisition locks renewed while their job was still running")
                .register(registry);
        this.renewalsPerJob = DistributionSummary.builder(RENEWALS_PER_JOB)
                .description("How often a job had its acquisition lock renewed before it finished")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        this.lateCompletions = Counter.builder(LATE_COMPLETIONS)
                .description("Jobs that finished after their record had been given up")
                .register(registry);
    }

    public void renewed() {
        renewed.increment();
    }

    public void expired(final LockLoss loss) {
        registry.counter(EXPIRED, REASON, loss.tag()).increment();
    }

    public void jobFinished(final int renewals) {
        renewalsPerJob.record(renewals);
    }

    public void completedAfterTheLockWasGivenUp() {
        lateCompletions.increment();
    }
}
