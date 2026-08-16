package dev.blumek.inference.worker.inflight;

import static java.util.Objects.requireNonNull;

public record Completion(RecordRef ref, Disposition disposition) {

    public Completion {
        requireNonNull(ref);
        requireNonNull(disposition);
    }
}
