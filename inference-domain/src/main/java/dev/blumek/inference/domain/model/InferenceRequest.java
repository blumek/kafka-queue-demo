package dev.blumek.inference.domain.model;

import java.time.Instant;

public record InferenceRequest(JobId jobId,
                               ModelId model,
                               String prompt,
                               int maxTokens,
                               Instant submittedAt) {}
