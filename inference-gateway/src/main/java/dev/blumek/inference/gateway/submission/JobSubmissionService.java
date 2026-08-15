package dev.blumek.inference.gateway.submission;

import java.util.concurrent.CompletableFuture;

class JobSubmissionService {
    private final JobPublisher publisher;
    private final SubmitJobMapper mapper;

    JobSubmissionService(final JobPublisher publisher, final SubmitJobMapper mapper) {
        this.publisher = publisher;
        this.mapper = mapper;
    }

    CompletableFuture<Submission> submit(final SubmitJobRequest request) {
        final var job = mapper.toInferenceRequest(request);
        return publisher
                .publish(job)
                .thenApply(outcome -> new Submission(job.jobId(), outcome));
    }
}
