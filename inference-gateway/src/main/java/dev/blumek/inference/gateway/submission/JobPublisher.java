package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.InferenceRequest;

import java.util.concurrent.CompletableFuture;

public interface JobPublisher {
    CompletableFuture<PublishOutcome> publish(InferenceRequest request);
}
