package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.ModelId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JobSubmissionServiceTest {
    private static final Instant NOW = Instant.parse("2026-08-15T10:15:30Z");
    private static final Duration RETRY_AFTER = Duration.ofSeconds(5);
    private static final String KEY = "7d1f4a58-1c9e-4c2f-9a3b-2f6e5d4c3b2a";

    private final JobPublisher publisher = mock(JobPublisher.class);

    private final JobSubmissionService service =
            new JobSubmissionService(publisher, new SubmitJobMapper(Clock.fixed(NOW, ZoneOffset.UTC)));

    @Test
    void publishesTheMappedRequestRatherThanTheCommandItWasGiven() {
        givenThePublisherAnswers(new PublishOutcome.Accepted());

        whenSubmitted();

        assertThat(whenThePublishedRequestIsCaptured())
                .satisfies(request -> assertThat(request.model()).isEqualTo(new ModelId("llama3:8b")))
                .satisfies(request -> assertThat(request.prompt()).isEqualTo("why is the sky blue?"))
                .satisfies(request -> assertThat(request.maxTokens()).isEqualTo(128))
                .satisfies(request -> assertThat(request.submittedAt()).isEqualTo(NOW));
    }

    @Test
    void reportsTheJobIdItMintedForTheSubmission() {
        givenThePublisherAnswers(new PublishOutcome.Accepted());

        final var actualSubmission = whenSubmitted().join();

        assertThat(actualSubmission.jobId()).isEqualTo(whenThePublishedRequestIsCaptured().jobId());
    }

    @Test
    void reportsTheOutcomeThePublisherReturned() {
        givenThePublisherAnswers(new PublishOutcome.Unavailable(RETRY_AFTER));

        final var actualSubmission = whenSubmitted().join();

        assertThat(actualSubmission.outcome()).isEqualTo(new PublishOutcome.Unavailable(RETRY_AFTER));
    }

    @Test
    void staysPendingWhileThePublisherHasNotAnswered() {
        when(publisher.publish(any())).thenReturn(new CompletableFuture<>());

        final var actualSubmission = whenSubmitted();

        assertThat(actualSubmission).isNotDone();
    }

    @Test
    void mintsADistinctJobIdForEverySubmissionThatCarriesNoKey() {
        givenThePublisherAnswers(new PublishOutcome.Accepted());

        final var actualFirst = whenSubmitted().join();
        final var actualSecond = whenSubmitted().join();

        assertThat(actualFirst.jobId()).isNotEqualTo(actualSecond.jobId());
    }

    @Test
    void reportsTheSameJobIdWhenARetryCarriesTheSameKey() {
        givenThePublisherAnswers(new PublishOutcome.Accepted());

        final var actualFirst = whenSubmittedWithKey(KEY).join();
        final var actualSecond = whenSubmittedWithKey(KEY).join();

        assertThat(actualFirst.jobId()).isEqualTo(actualSecond.jobId());
    }

    @Test
    void rejectsANullSubmission() {
        assertThatNullPointerException().isThrownBy(() -> service.submit(null));
    }

    private CompletableFuture<Submission> whenSubmitted() {
        return service.submit(givenASubmission());
    }

    private CompletableFuture<Submission> whenSubmittedWithKey(final String idempotencyKey) {
        return service.submit(new SubmissionCommand("llama3:8b", "why is the sky blue?", 128, idempotencyKey));
    }

    private void givenThePublisherAnswers(final PublishOutcome outcome) {
        when(publisher.publish(any())).thenReturn(CompletableFuture.completedFuture(outcome));
    }

    private InferenceRequest whenThePublishedRequestIsCaptured() {
        final var captor = ArgumentCaptor.forClass(InferenceRequest.class);
        verify(publisher).publish(captor.capture());
        return captor.getValue();
    }

    private static SubmissionCommand givenASubmission() {
        return new SubmissionCommand("llama3:8b", "why is the sky blue?", 128, null);
    }
}
