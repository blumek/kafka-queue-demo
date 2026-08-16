package dev.blumek.inference.worker.processing;

import dev.blumek.inference.domain.port.EngineOutcome;

public final class RedeliverOnFailureClassifier implements DispositionClassifier {

    @Override
    public Disposition classify(final EngineOutcome outcome, final int deliveryCount) {
        return switch (outcome) {
            case EngineOutcome.Completed _   -> new Disposition.Accept();
            case EngineOutcome.Failed failed -> new Disposition.Release(reasonFor(failed, deliveryCount));
        };
    }

    private static String reasonFor(final EngineOutcome.Failed failed, final int deliveryCount) {
        return "%s on delivery %d".formatted(failed, deliveryCount);
    }
}
