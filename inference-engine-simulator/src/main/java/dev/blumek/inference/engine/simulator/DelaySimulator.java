package dev.blumek.inference.engine.simulator;

import java.time.Duration;

final class DelaySimulator {

    void simulate(final Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (final InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while simulating inference", exception);
        }
    }
}
