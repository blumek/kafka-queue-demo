package dev.blumek.inference.domain.port;

import dev.blumek.inference.domain.model.InferenceResult;

import java.time.Duration;
import java.util.Optional;

public sealed interface EngineOutcome {

    record Completed(InferenceResult result) implements EngineOutcome {}

    sealed interface Failed extends EngineOutcome {
        record Timeout(Duration elapsed)                  implements Failed {}
        record RateLimited(Optional<Duration> retryAfter) implements Failed {}
        record MalformedInput(String reason)              implements Failed {}
        record Unavailable(String detail)                 implements Failed {}
    }
}
