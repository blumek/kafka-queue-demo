package dev.blumek.inference.gateway.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.gateway.submission.JobPublisher;
import dev.blumek.inference.gateway.submission.PublishOutcome;
import dev.blumek.inference.gateway.tracing.TraceOrigin;
import dev.blumek.inference.messaging.InferenceTopics;
import dev.blumek.inference.messaging.JobHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

class KafkaJobPublisher implements JobPublisher {
    private final KafkaTemplate<String, InferenceRequest> template;
    private final TraceOrigin traceOrigin;
    private final Executor sends;
    private final Duration publishTimeout;
    private final Duration retryAfter;

    KafkaJobPublisher(final KafkaTemplate<String, InferenceRequest> template,
                      final TraceOrigin traceOrigin,
                      final Executor sends,
                      final Duration publishTimeout,
                      final Duration retryAfter) {
        this.template = template;
        this.traceOrigin = traceOrigin;
        this.sends = sends;
        this.publishTimeout = publishTimeout;
        this.retryAfter = retryAfter;
    }

    @Override
    public CompletableFuture<PublishOutcome> publish(final InferenceRequest request) {
        requireNonNull(request);
        return sendOffTheCallingThread(recordFor(request))
                .completeOnTimeout(unavailable(), publishTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private ProducerRecord<String, InferenceRequest> recordFor(final InferenceRequest request) {
        final var record = new ProducerRecord<>(InferenceTopics.JOBS, request.jobId().value(), request);
        traceOrigin.traceparent().ifPresent(traceparent -> JobHeaders.putTraceparent(record.headers(), traceparent));
        return record;
    }

    private CompletableFuture<PublishOutcome> sendOffTheCallingThread(final ProducerRecord<String, InferenceRequest> record) {
        try {
            return CompletableFuture.supplyAsync(() -> send(record), sends).thenCompose(outcome -> outcome);
        } catch (final RejectedExecutionException exception) {
            return CompletableFuture.completedFuture(unavailable());
        }
    }

    private CompletableFuture<PublishOutcome> send(final ProducerRecord<String, InferenceRequest> record) {
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
                return unavailable();
            }
            deepest = current;
        }
        return new PublishOutcome.Rejected(String.valueOf(deepest));
    }

    private PublishOutcome unavailable() {
        return new PublishOutcome.Unavailable(retryAfter);
    }

    private static Throwable causeOf(final Throwable throwable) {
        return throwable.getCause() == throwable ? null : throwable.getCause();
    }
}
