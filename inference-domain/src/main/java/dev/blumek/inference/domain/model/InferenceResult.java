package dev.blumek.inference.domain.model;

import java.time.Duration;
import java.time.Instant;

public record InferenceResult(JobId jobId,
                              ModelId model,
                              String completion,
                              int promptTokens,
                              int completionTokens,
                              Duration engineLatency,
                              Instant completedAt) {}
