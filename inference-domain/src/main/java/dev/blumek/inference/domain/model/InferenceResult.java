package dev.blumek.inference.domain.model;

import java.time.Duration;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public record InferenceResult(JobId jobId,
                              ModelId model,
                              String completion,
                              int promptTokens,
                              int completionTokens,
                              Duration engineLatency,
                              Instant completedAt) {

    public InferenceResult {
        requireNonNull(jobId);
        requireNonNull(model);
        requireNonNull(completion);
        requireNonNull(engineLatency);
        requireNonNull(completedAt);
    }
}
