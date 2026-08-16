package dev.blumek.inference.worker.processing;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.port.InferenceEngine;

public final class JobProcessor {
    private final InferenceEngine engine;
    private final DispositionClassifier classifier;

    public JobProcessor(final InferenceEngine engine, final DispositionClassifier classifier) {
        this.engine = engine;
        this.classifier = classifier;
    }

    public Disposition process(final InferenceRequest request, final int deliveryCount) {
        return classifier.classify(engine.run(request), deliveryCount);
    }
}
