package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.JobId;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/jobs")
class JobController {
    private final JobSubmissionService submissions;

    JobController(final JobSubmissionService submissions) {
        this.submissions = submissions;
    }

    @PostMapping
    CompletableFuture<ResponseEntity<JobAcceptedResponse>> submit(@Valid @RequestBody final SubmitJobRequest request) {
        return submissions.submit(request).thenApply(JobController::respondTo);
    }

    private static ResponseEntity<JobAcceptedResponse> respondTo(final Submission submission) {
        return switch (submission.outcome()) {
            case PublishOutcome.Accepted ignored -> accepted(submission.jobId());
            case PublishOutcome.Unavailable unavailable -> throw new PublisherUnavailableException(unavailable.retryAfter());
            case PublishOutcome.Rejected rejected -> throw new PublishRejectedException(rejected.reason());
        };
    }

    private static ResponseEntity<JobAcceptedResponse> accepted(final JobId jobId) {
        final var statusUrl = statusUrl(jobId);
        return ResponseEntity.accepted().location(statusUrl).body(new JobAcceptedResponse(jobId.value(), statusUrl));
    }

    private static URI statusUrl(final JobId jobId) {
        return UriComponentsBuilder.fromPath("/jobs").pathSegment(jobId.value()).build().toUri();
    }
}
