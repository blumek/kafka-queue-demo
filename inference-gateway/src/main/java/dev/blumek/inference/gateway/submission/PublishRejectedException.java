package dev.blumek.inference.gateway.submission;

class PublishRejectedException extends RuntimeException {
    PublishRejectedException(final String reason) {
        super(reason);
    }
}
