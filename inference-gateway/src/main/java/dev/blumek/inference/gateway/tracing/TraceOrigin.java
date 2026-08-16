package dev.blumek.inference.gateway.tracing;

import java.util.Optional;

@FunctionalInterface
public interface TraceOrigin {
    Optional<String> traceparent();
}
