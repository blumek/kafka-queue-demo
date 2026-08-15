package dev.blumek.inference.gateway.submission;

import java.time.Duration;

class PublisherUnavailableException extends RuntimeException {
    private final Duration retryAfter;

    PublisherUnavailableException(final Duration retryAfter) {
        super("the job could not be queued");
        this.retryAfter = retryAfter;
    }

    Duration retryAfter() {
        return retryAfter;
    }
}
