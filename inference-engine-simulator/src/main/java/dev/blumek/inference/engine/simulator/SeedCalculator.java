package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.JobId;

final class SeedCalculator {
    private final long runSeed;

    SeedCalculator(final long runSeed) {
        this.runSeed = runSeed;
    }

    long calculate(final JobId jobId) {
        return runSeed * 31 + jobId.value().hashCode();
    }
}
