package dev.blumek.inference.worker.processing;

import dev.blumek.inference.domain.port.EngineOutcome;

public interface DispositionClassifier {
    Disposition classify(EngineOutcome outcome, int deliveryCount);
}
