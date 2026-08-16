package dev.blumek.inference.gateway.submission;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class SubmissionCommandTest {
    private static final String KEY = "7d1f4a58-1c9e-4c2f-9a3b-2f6e5d4c3b2a";

    @Test
    void offersTheKeyItWasGiven() {
        assertThat(givenACommandKeyedBy(KEY).idempotencyKey()).contains(KEY);
    }

    @Test
    void offersNothingWhenNoKeyCameWithTheSubmission() {
        assertThat(givenACommandKeyedBy(null).idempotencyKey()).isEmpty();
    }

    @Test
    void rejectsAMissingModel() {
        assertThatNullPointerException()
                .isThrownBy(() -> new SubmissionCommand(null, "why is the sky blue?", 128, null));
    }

    @Test
    void rejectsAMissingPrompt() {
        assertThatNullPointerException().isThrownBy(() -> new SubmissionCommand("llama3:8b", null, 128, null));
    }

    private static SubmissionCommand givenACommandKeyedBy(final String idempotencyKey) {
        return new SubmissionCommand("llama3:8b", "why is the sky blue?", 128, idempotencyKey);
    }
}
