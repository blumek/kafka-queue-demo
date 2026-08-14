package dev.blumek.inference.domain.port;

import dev.blumek.inference.domain.model.InferenceRequest;

public interface InferenceEngine {
    EngineOutcome run(InferenceRequest request);
}
