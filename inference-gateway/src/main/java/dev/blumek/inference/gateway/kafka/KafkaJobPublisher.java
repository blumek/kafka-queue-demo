package dev.blumek.inference.gateway.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.gateway.submission.JobPublisher;
import dev.blumek.inference.gateway.submission.PublishOutcome;
import dev.blumek.inference.messaging.InferenceTopics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

class KafkaJobPublisher implements JobPublisher {
    private final KafkaTemplate<String, InferenceRequest> template;
    private final Duration retryAfter;

    KafkaJobPublisher(final KafkaTemplate<String, InferenceRequest> template, final Duration retryAfter) {
        this.template = requireNonNull(template);
        this.retryAfter = requireNonNull(retryAfter);
    }

    @Override
    public CompletableFuture<PublishOutcome> publish(final InferenceRequest request) {
        requireNonNull(request);
        final var record = new ProducerRecord<>(InferenceTopics.JOBS, request.jobId().value(), request);
        try {
            return template.send(record)
                    .<PublishOutcome>handle((result, throwable) -> throwable == null
                            ? new PublishOutcome.Accepted()
                            : outcomeFor(throwable));
        } catch (final RuntimeException e) {
            // KafkaTemplate rethrows an immediate failure instead of completing the future, and send()
            // itself throws once the buffer is full or metadata never arrives within max.block.ms.
            return CompletableFuture.completedFuture(outcomeFor(e));
        }
    }

    // Both failure paths arrive wrapped: the future carries a KafkaProducerException and the synchronous
    // path a KafkaException, each holding the real cause. Classifying the wrapper would turn every
    // retriable failure into a permanent rejection, so the whole chain is inspected.
    private PublishOutcome outcomeFor(final Throwable throwable) {
        var deepest = throwable;
        for (var current = throwable; current != null; current = causeOf(current)) {
            if (current instanceof RetriableException) {
                return new PublishOutcome.Unavailable(retryAfter);
            }
            deepest = current;
        }
        return new PublishOutcome.Rejected(String.valueOf(deepest));
    }

    private static Throwable causeOf(final Throwable throwable) {
        return throwable.getCause() == throwable ? null : throwable.getCause();
    }
}
