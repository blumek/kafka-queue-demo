package dev.blumek.inference.gateway.submission;

import java.time.Duration;

public sealed interface PublishOutcome {
    record Accepted()                       implements PublishOutcome {}
    record Unavailable(Duration retryAfter) implements PublishOutcome {}
    record Rejected(String reason)          implements PublishOutcome {}
}
