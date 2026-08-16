package dev.blumek.inference.worker.kafka;

public interface ConsumptionStrategy {
    void consume();
    void stop();
}
