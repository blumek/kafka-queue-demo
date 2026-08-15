package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.JobId;

record Submission(JobId jobId, PublishOutcome outcome) {}
