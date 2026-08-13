package dev.blumek.inference.domain.port;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.InferenceResult;

public interface InferenceEngine {
    InferenceResult run(InferenceRequest request);
}
