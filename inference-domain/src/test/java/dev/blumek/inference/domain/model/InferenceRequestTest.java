package dev.blumek.inference.domain.model;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class InferenceRequestTest {
    private static final String PROMPT = "why is the sky blue?";
    private static final int MAX_TOKENS = 256;
    private static final Instant SUBMITTED_AT = Instant.parse("2026-08-14T10:15:30.123456789Z");

    @Test
    void keepsGivenValues() {
        final var actualRequest = givenRequest();

        assertThat(actualRequest.jobId()).isEqualTo(givenJobId());
        assertThat(actualRequest.model()).isEqualTo(givenModel());
        assertThat(actualRequest.prompt()).isEqualTo(PROMPT);
        assertThat(actualRequest.maxTokens()).isEqualTo(MAX_TOKENS);
        assertThat(actualRequest.submittedAt()).isEqualTo(SUBMITTED_AT);
    }

    private InferenceRequest givenRequest() {
        return new InferenceRequest(givenJobId(), givenModel(), PROMPT, MAX_TOKENS, SUBMITTED_AT);
    }

    private JobId givenJobId() {
        return new JobId("job-42");
    }

    private ModelId givenModel() {
        return new ModelId("llama3:8b");
    }

    @Test
    void rejectsNullJobId() {
        final var actualThrown = catchThrowable(
                () -> new InferenceRequest(null, givenModel(), PROMPT, MAX_TOKENS, SUBMITTED_AT));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullModel() {
        final var actualThrown = catchThrowable(
                () -> new InferenceRequest(givenJobId(), null, PROMPT, MAX_TOKENS, SUBMITTED_AT));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullPrompt() {
        final var actualThrown = catchThrowable(
                () -> new InferenceRequest(givenJobId(), givenModel(), null, MAX_TOKENS, SUBMITTED_AT));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullSubmittedAt() {
        final var actualThrown = catchThrowable(
                () -> new InferenceRequest(givenJobId(), givenModel(), PROMPT, MAX_TOKENS, null));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }
}
