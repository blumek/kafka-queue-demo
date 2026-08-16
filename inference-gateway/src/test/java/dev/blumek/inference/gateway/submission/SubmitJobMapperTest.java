package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.ModelId;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class SubmitJobMapperTest {
    private static final Instant NOW = Instant.parse("2026-08-15T10:15:30Z");
    private static final String KEY = "7d1f4a58-1c9e-4c2f-9a3b-2f6e5d4c3b2a";
    private static final String ANOTHER_KEY = "1b2c3d4e-5f60-4a71-8b92-0c1d2e3f4a5b";

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
    void mintsADistinctJobIdForEverySubmissionThatCarriesNoKey() {
        final var actualFirst = mapper.toInferenceRequest(givenASubmission());
        final var actualSecond = mapper.toInferenceRequest(givenASubmission());

        assertThat(actualFirst.jobId()).isNotEqualTo(actualSecond.jobId());
    }

    @Test
    void reusesTheJobIdWhenTheSameKeyComesBack() {
        final var actualFirst = mapper.toInferenceRequest(givenASubmissionKeyedBy(KEY));
        final var actualSecond = mapper.toInferenceRequest(givenASubmissionKeyedBy(KEY));

        assertThat(actualFirst.jobId()).isEqualTo(actualSecond.jobId());
    }

    @Test
    void derivesTheJobIdFromTheKeyAloneRatherThanFromWhatWasSubmittedWithIt() {
        final var actualFirst = mapper.toInferenceRequest(givenASubmissionKeyedBy(KEY));
        final var actualSecond = mapper.toInferenceRequest(
                new SubmissionCommand("mistral", "a different prompt", 64, KEY));

        assertThat(actualFirst.jobId()).isEqualTo(actualSecond.jobId());
    }

    @Test
    void issuesDistinctJobIdsForDistinctKeys() {
        final var actualFirst = mapper.toInferenceRequest(givenASubmissionKeyedBy(KEY));
        final var actualSecond = mapper.toInferenceRequest(givenASubmissionKeyedBy(ANOTHER_KEY));

        assertThat(actualFirst.jobId()).isNotEqualTo(actualSecond.jobId());
    }

    @Test
    void takesTheKeyItselfAsTheJobIdRatherThanMintingOne() {
        final var actualRequest = mapper.toInferenceRequest(givenASubmissionKeyedBy(KEY));

        assertThat(actualRequest.jobId().value()).isEqualTo(KEY);
    }

    @Test
    void readsAKeyTheClientShoutedAsTheSameJobAsTheQuietOne() {
        final var actualShouted = mapper.toInferenceRequest(givenASubmissionKeyedBy(KEY.toUpperCase()));

        assertThat(actualShouted.jobId().value()).isEqualTo(KEY);
    }

    @Test
    void rejectsAKeyWithNothingInIt() {
        final var submission = givenASubmissionKeyedBy("   ");

        assertThatExceptionOfType(MalformedIdempotencyKeyException.class)
                .isThrownBy(() -> mapper.toInferenceRequest(submission));
    }

    @Test
    void rejectsAKeyThatIsNotAUuidSoItCannotSteerTheStatusUrl() {
        final var submission = givenASubmissionKeyedBy("../admin");

        assertThatExceptionOfType(MalformedIdempotencyKeyException.class)
                .isThrownBy(() -> mapper.toInferenceRequest(submission));
    }

    @Test
    void rejectsAModelIdTheDomainDoesNotAccept() {
        final var submission = new SubmissionCommand("Not A Model", "why is the sky blue?", 128, null);

        assertThatIllegalArgumentException().isThrownBy(() -> mapper.toInferenceRequest(submission));
    }

    @Test
    void rejectsANullSubmission() {
        assertThatNullPointerException().isThrownBy(() -> mapper.toInferenceRequest(null));
    }

    private static SubmissionCommand givenASubmission() {
        return new SubmissionCommand("llama3:8b", "why is the sky blue?", 128, null);
    }

    private static SubmissionCommand givenASubmissionKeyedBy(final String idempotencyKey) {
        return new SubmissionCommand("llama3:8b", "why is the sky blue?", 128, idempotencyKey);
    }
}
