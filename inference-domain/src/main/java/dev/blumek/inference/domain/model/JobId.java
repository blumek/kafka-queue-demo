package dev.blumek.inference.domain.model;

import java.util.UUID;

import static java.util.Objects.requireNonNull;

public record JobId(String value) {
    public JobId {
        requireNonNull(value);
    }

    public static JobId newId() {
        return new JobId(UUID.randomUUID().toString());
    }
}
