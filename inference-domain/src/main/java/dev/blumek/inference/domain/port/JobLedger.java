package dev.blumek.inference.domain.port;

import dev.blumek.inference.domain.model.InferenceResult;
import dev.blumek.inference.domain.model.JobId;

import java.time.Duration;

public interface JobLedger {
    ClaimOutcome acquire(JobId id, Duration ttl);
    void complete(JobId id, InferenceResult result, Duration retention);
    void abandon(JobId id);
}
