package dev.blumek.inference.engine.simulator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class PromptTokenEstimatorTest {
    private final PromptTokenEstimator estimator = new PromptTokenEstimator();

    @ParameterizedTest
    @CsvSource({"'0123456789abcdefghij', 5", "'abcd', 1", "'abcdefgh', 2", "'abcdefg', 1"})
    void countsFourCharactersPerToken(final String givenPrompt, final int expectedTokens) {
        final var actualTokens = estimator.estimate(givenPrompt);

        assertThat(actualTokens).isEqualTo(expectedTokens);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ab"})
    void neverFallsBelowASingleToken(final String givenPrompt) {
        final var actualTokens = estimator.estimate(givenPrompt);

        assertThat(actualTokens).isEqualTo(1);
    }

    @Test
    void growsMonotonicallyWithPromptLength() {
        final var givenLongPrompt = givenPrompt(400);
        final var givenShortPrompt = givenPrompt(40);

        final var actualTokens = estimator.estimate(givenLongPrompt);

        assertThat(actualTokens).isGreaterThan(estimator.estimate(givenShortPrompt));
    }

    private String givenPrompt(final int length) {
        return "a".repeat(length);
    }
}
