package dev.blumek.inference.gateway.submission;

class MalformedIdempotencyKeyException extends RuntimeException {
    MalformedIdempotencyKeyException(final Throwable cause) {
        super("the idempotency key was not a uuid", cause);
    }
}
