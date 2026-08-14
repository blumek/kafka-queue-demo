package dev.blumek.inference.domain.port;

import dev.blumek.inference.domain.model.InferenceResult;

import java.time.Instant;

public sealed interface ClaimOutcome {
    record Acquired()                          implements ClaimOutcome {}
    record InProgress(Instant claimedAt)       implements ClaimOutcome {}
    record Completed(InferenceResult result)   implements ClaimOutcome {}
}
