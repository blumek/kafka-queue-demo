package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.ModelId;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class SubmitJobMapperTest {
    private static final Instant NOW = Instant.parse("2026-08-15T10:15:30Z");

    private final SubmitJobMapper mapper = new SubmitJobMapper(Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void carriesTheModelTheClientAskedFor() {
        final var actualRequest = mapper.toInferenceRequest(givenASubmission());

        assertThat(actualRequest.model()).isEqualTo(new ModelId("llama3:8b"));
    }

    @Test
    void carriesThePromptTheClientSent() {
        final var actualRequest = mapper.toInferenceRequest(givenASubmission());

        assertThat(actualRequest.prompt()).isEqualTo("why is the sky blue?");
    }

    @Test
    void carriesTheTokenBudgetTheClientAskedFor() {
        final var actualRequest = mapper.toInferenceRequest(givenASubmission());

        assertThat(actualRequest.maxTokens()).isEqualTo(128);
    }

    @Test
    void stampsTheSubmissionTimeFromTheClockRatherThanTheClient() {
        final var actualRequest = mapper.toInferenceRequest(givenASubmission());

        assertThat(actualRequest.submittedAt()).isEqualTo(NOW);
    }

    @Test
    void mintsAJobIdTheClientCannotChoose() {
        final var actualRequest = mapper.toInferenceRequest(givenASubmission());

        assertThat(actualRequest.jobId().value()).isNotBlank();
    }

    @Test
    void mintsADistinctJobIdForEverySubmission() {
        final var actualFirst = mapper.toInferenceRequest(givenASubmission());
        final var actualSecond = mapper.toInferenceRequest(givenASubmission());

        assertThat(actualFirst.jobId()).isNotEqualTo(actualSecond.jobId());
    }

    @Test
    void rejectsAModelIdTheDomainDoesNotAccept() {
        final var submission = new SubmitJobRequest("Not A Model", "why is the sky blue?", 128);

        assertThatIllegalArgumentException().isThrownBy(() -> mapper.toInferenceRequest(submission));
    }

    @Test
    void rejectsANullSubmission() {
        assertThatNullPointerException().isThrownBy(() -> mapper.toInferenceRequest(null));
    }

    private static SubmitJobRequest givenASubmission() {
        return new SubmitJobRequest("llama3:8b", "why is the sky blue?", 128);
    }
}
