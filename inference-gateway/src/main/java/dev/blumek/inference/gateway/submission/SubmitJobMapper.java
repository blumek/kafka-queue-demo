package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;

import java.time.Clock;

import static java.util.Objects.requireNonNull;

class SubmitJobMapper {
    private final Clock clock;

    SubmitJobMapper(final Clock clock) {
        this.clock = clock;
    }

    InferenceRequest toInferenceRequest(final SubmitJobRequest submission) {
        requireNonNull(submission);
        return new InferenceRequest(JobId.newId(),
                new ModelId(submission.model()),
                submission.prompt(),
                submission.maxTokens(),
                clock.instant());
    }
}
