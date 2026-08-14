package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.JobId;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class AttemptTracker {
    private static final int MAX_TRACKED_JOBS = 100_000;

    private final ConcurrentMap<JobId, Integer> attempts = new ConcurrentHashMap<>();

    int next(final JobId jobId) {
        if (attempts.size() >= MAX_TRACKED_JOBS) {
            attempts.clear();
        }
        return attempts.merge(jobId, 1, Integer::sum);
    }
}
