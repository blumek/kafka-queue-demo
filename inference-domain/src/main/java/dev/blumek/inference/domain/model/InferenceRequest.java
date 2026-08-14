package dev.blumek.inference.domain.model;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

public record InferenceRequest(JobId jobId,
                               ModelId model,
                               String prompt,
                               int maxTokens,
                               Instant submittedAt) {

    public InferenceRequest {
        requireNonNull(jobId);
        requireNonNull(model);
        requireNonNull(prompt);
        requireNonNull(submittedAt);
    }
}
