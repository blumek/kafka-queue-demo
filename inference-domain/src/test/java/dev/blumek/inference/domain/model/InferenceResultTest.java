package dev.blumek.inference.domain.model;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class InferenceResultTest {
    private static final String COMPLETION = "[sim llama3:8b] because of rayleigh scattering";
    private static final int PROMPT_TOKENS = 5;
    private static final int COMPLETION_TOKENS = 7;
    private static final Duration ENGINE_LATENCY = Duration.ofMillis(7);
    private static final Instant COMPLETED_AT = Instant.parse("2026-08-14T10:15:30.123456789Z");

    @Test
    void keepsGivenValues() {
        final var actualResult = givenResult();

        assertThat(actualResult.jobId()).isEqualTo(givenJobId());
        assertThat(actualResult.model()).isEqualTo(givenModel());
        assertThat(actualResult.completion()).isEqualTo(COMPLETION);
        assertThat(actualResult.promptTokens()).isEqualTo(PROMPT_TOKENS);
        assertThat(actualResult.completionTokens()).isEqualTo(COMPLETION_TOKENS);
        assertThat(actualResult.engineLatency()).isEqualTo(ENGINE_LATENCY);
        assertThat(actualResult.completedAt()).isEqualTo(COMPLETED_AT);
    }

    private InferenceResult givenResult() {
        return new InferenceResult(givenJobId(),
                                   givenModel(),
                                   COMPLETION,
                                   PROMPT_TOKENS,
                                   COMPLETION_TOKENS,
                                   ENGINE_LATENCY,
                                   COMPLETED_AT);
    }

    private JobId givenJobId() {
        return new JobId("job-42");
    }

    private ModelId givenModel() {
        return new ModelId("llama3:8b");
    }

    @Test
    void rejectsNullJobId() {
        final var actualThrown = catchThrowable(() -> new InferenceResult(
                null, givenModel(), COMPLETION, PROMPT_TOKENS, COMPLETION_TOKENS, ENGINE_LATENCY, COMPLETED_AT));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullModel() {
        final var actualThrown = catchThrowable(() -> new InferenceResult(
                givenJobId(), null, COMPLETION, PROMPT_TOKENS, COMPLETION_TOKENS, ENGINE_LATENCY, COMPLETED_AT));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullCompletion() {
        final var actualThrown = catchThrowable(() -> new InferenceResult(
                givenJobId(), givenModel(), null, PROMPT_TOKENS, COMPLETION_TOKENS, ENGINE_LATENCY, COMPLETED_AT));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullEngineLatency() {
        final var actualThrown = catchThrowable(() -> new InferenceResult(
                givenJobId(), givenModel(), COMPLETION, PROMPT_TOKENS, COMPLETION_TOKENS, null, COMPLETED_AT));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsNullCompletedAt() {
        final var actualThrown = catchThrowable(() -> new InferenceResult(
                givenJobId(), givenModel(), COMPLETION, PROMPT_TOKENS, COMPLETION_TOKENS, ENGINE_LATENCY, null));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }
}
