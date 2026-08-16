package dev.blumek.inference.gateway.submission;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

record SubmissionCommand(String model, String prompt, int maxTokens, String key) {
    SubmissionCommand {
        requireNonNull(model);
        requireNonNull(prompt);
    }

    Optional<String> idempotencyKey() {
        return Optional.ofNullable(key);
    }
}
