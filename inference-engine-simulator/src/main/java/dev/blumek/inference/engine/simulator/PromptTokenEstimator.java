package dev.blumek.inference.engine.simulator;

final class PromptTokenEstimator {
    private static final int CHARS_PER_TOKEN = 4;

    int estimate(final String prompt) {
        return Math.max(1, prompt.length() / CHARS_PER_TOKEN);
    }
}
