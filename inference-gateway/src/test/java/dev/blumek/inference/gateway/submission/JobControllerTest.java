package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.JobId;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.test.web.servlet.assertj.MvcTestResult;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JobControllerTest {
    private static final String JOB_ID = "8b9f2a1c-0d3e-4f5a-9b6c-7d8e9f0a1b2c";
    private static final URI STATUS_URL = URI.create("/jobs/" + JOB_ID);
    private static final Duration RETRY_AFTER = Duration.ofSeconds(5);
    private static final String SUBMISSION = """
            {"model": "llama3:8b", "prompt": "why is the sky blue?", "maxTokens": 128}""";

    private final JobSubmissionService submissions = mock(JobSubmissionService.class);

    private final MockMvcTester mvc = MockMvcTester.of(new JobController(submissions));

    @Test
    void answersAnAcceptedSubmissionWithTwoHundredAndTwo() {
        givenTheSubmissionIs(new PublishOutcome.Accepted());

        assertThat(whenAJobIsSubmitted()).hasStatus(HttpStatus.ACCEPTED);
    }

    @Test
    void pointsTheLocationHeaderAtTheStatusUrlOfTheNewJob() {
        givenTheSubmissionIs(new PublishOutcome.Accepted());

        assertThat(whenAJobIsSubmitted()).hasHeader(HttpHeaders.LOCATION, STATUS_URL.toString());
    }

    @Test
    void answersWithTheJobIdAndTheStatusUrlItIssued() {
        givenTheSubmissionIs(new PublishOutcome.Accepted());

        assertThat(whenAJobIsSubmitted()).bodyJson().isLenientlyEqualTo("""
                {"jobId": "%s", "statusUrl": "%s"}""".formatted(JOB_ID, STATUS_URL));
    }

    @Test
    void bindsTheWireRecordAndHandsItToTheService() {
        givenTheSubmissionIs(new PublishOutcome.Accepted());

        whenAJobIsSubmitted();

        final var captor = ArgumentCaptor.forClass(SubmitJobRequest.class);
        verify(submissions).submit(captor.capture());
        assertThat(captor.getValue()).isEqualTo(new SubmitJobRequest("llama3:8b", "why is the sky blue?", 128));
    }

    @Test
    void asksTheClientToRetryLaterWhenTheBrokerIsUnavailable() {
        givenTheSubmissionIs(new PublishOutcome.Unavailable(RETRY_AFTER));

        assertThat(whenAJobIsSubmitted()).hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .hasHeader(HttpHeaders.RETRY_AFTER, "5");
    }

    @Test
    void answersWithAServerErrorWhenThePublishWasRejectedOutright() {
        givenTheSubmissionIs(new PublishOutcome.Rejected("RecordTooLargeException"));

        assertThat(whenAJobIsSubmitted()).hasStatus(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void tellsAClientItCannotRetryARejectedPublish() {
        givenTheSubmissionIs(new PublishOutcome.Rejected("RecordTooLargeException"));

        assertThat(whenAJobIsSubmitted()).doesNotContainHeader(HttpHeaders.RETRY_AFTER);
    }

    @Test
    void doesNotLeakTheRejectionReasonToTheClient() {
        givenTheSubmissionIs(new PublishOutcome.Rejected("RecordTooLargeException"));

        assertThat(whenAJobIsSubmitted()).body().isEmpty();
    }

    private void givenTheSubmissionIs(final PublishOutcome outcome) {
        when(submissions.submit(any()))
                .thenReturn(CompletableFuture.completedFuture(new Submission(new JobId(JOB_ID), outcome)));
    }

    private MvcTestResult whenAJobIsSubmitted() {
        return mvc.post()
                .uri("/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .content(SUBMISSION)
                .exchange();
    }
}
