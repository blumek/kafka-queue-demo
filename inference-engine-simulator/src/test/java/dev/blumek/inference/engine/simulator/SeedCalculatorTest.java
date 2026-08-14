package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.JobId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SeedCalculatorTest {
    private static final long RUN_SEED = 1_234L;

    private final SeedCalculator seeds = new SeedCalculator(RUN_SEED);

    @Test
    void isStableForTheSameJobUnderTheSameRunSeed() {
        final var givenJobId = givenJobId();

        final var actualSeed = seeds.calculate(givenJobId);

        assertThat(actualSeed).isEqualTo(new SeedCalculator(RUN_SEED).calculate(givenJobId));
    }

    private JobId givenJobId() {
        return new JobId("job-a");
    }

    @Test
    void divergesBetweenJobsUnderTheSameRunSeed() {
        final var givenJobId = givenJobId();
        final var givenOtherJobId = givenOtherJobId();

        final var actualSeed = seeds.calculate(givenJobId);

        assertThat(actualSeed).isNotEqualTo(seeds.calculate(givenOtherJobId));
    }

    private JobId givenOtherJobId() {
        return new JobId("job-b");
    }

    @Test
    void divergesForTheSameJobWhenTheRunSeedChanges() {
        final var givenJobId = givenJobId();

        final var actualSeed = seeds.calculate(givenJobId);

        assertThat(actualSeed).isNotEqualTo(new SeedCalculator(RUN_SEED + 1).calculate(givenJobId));
    }
}
