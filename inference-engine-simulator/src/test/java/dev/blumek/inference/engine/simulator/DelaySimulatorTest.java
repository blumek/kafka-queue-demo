package dev.blumek.inference.engine.simulator;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class DelaySimulatorTest {
    private static final Duration DELAY = Duration.ofMillis(50);

    private final DelaySimulator delays = new DelaySimulator();

    @Test
    void blocksForTheGivenDuration() {
        final var givenStart = System.nanoTime();

        delays.simulate(DELAY);

        assertThat(Duration.ofNanos(System.nanoTime() - givenStart)).isGreaterThanOrEqualTo(DELAY);
    }

    @Test
    void returnsImmediatelyForAZeroDuration() {
        final var givenStart = System.nanoTime();

        delays.simulate(Duration.ZERO);

        assertThat(Duration.ofNanos(System.nanoTime() - givenStart)).isLessThan(DELAY);
    }

    @Test
    void reportsInterruptionAsFailure() {
        Thread.currentThread().interrupt();

        final var actualThrown = catchThrowable(() -> delays.simulate(DELAY));

        assertThat(actualThrown).isInstanceOf(IllegalStateException.class).hasCauseInstanceOf(InterruptedException.class);
        assertThat(Thread.interrupted()).isTrue();
    }
}
