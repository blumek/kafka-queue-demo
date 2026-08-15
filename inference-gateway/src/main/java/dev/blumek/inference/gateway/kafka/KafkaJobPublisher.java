package dev.blumek.inference.gateway.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.gateway.submission.JobPublisher;
import dev.blumek.inference.gateway.submission.PublishOutcome;
import dev.blumek.inference.messaging.InferenceTopics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

class KafkaJobPublisher implements JobPublisher {
    private final KafkaTemplate<String, InferenceRequest> template;
    private final Duration retryAfter;

    KafkaJobPublisher(final KafkaTemplate<String, InferenceRequest> template, final Duration retryAfter) {
        this.template = template;
        this.retryAfter = retryAfter;
    }

    @Override
    public CompletableFuture<PublishOutcome> publish(final InferenceRequest request) {
        requireNonNull(request);
        final var record = new ProducerRecord<>(InferenceTopics.JOBS, request.jobId().value(), request);
        return send(record);
    }

    private CompletableFuture<PublishOutcome> send(ProducerRecord<String, InferenceRequest> record) {
        try {
            return template.send(record).handle(handleResult());
        } catch (final RuntimeException exception) {
            return CompletableFuture.completedFuture(outcomeFor(exception));
        }
    }

    private BiFunction<SendResult<String, InferenceRequest>, Throwable, PublishOutcome> handleResult() {
        return (_, throwable) -> throwable == null
                ? new PublishOutcome.Accepted()
                : outcomeFor(throwable);
    }

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
