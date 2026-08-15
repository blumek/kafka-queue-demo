package dev.blumek.inference.gateway.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.gateway.submission.PublishOutcome;
import dev.blumek.inference.messaging.InferenceTopics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaProducerException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaJobPublisherTest {
    private static final String JOB_ID = "8b9f2a1c-0d3e-4f5a-9b6c-7d8e9f0a1b2c";
    private static final Duration RETRY_AFTER = Duration.ofSeconds(5);

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, InferenceRequest> template = mock(KafkaTemplate.class);

    private final KafkaJobPublisher publisher = new KafkaJobPublisher(template, RETRY_AFTER);

    @Test
    void keysEveryRecordByItsJobId() {
        givenTheBrokerAcknowledges();

        publisher.publish(givenARequest());

        assertThat(whenTheRecordIsCaptured().key()).isEqualTo(JOB_ID);
    }

    @Test
    void publishesToTheJobsTopic() {
        givenTheBrokerAcknowledges();

        publisher.publish(givenARequest());

        assertThat(whenTheRecordIsCaptured().topic()).isEqualTo(InferenceTopics.JOBS);
    }

    @Test
    void leavesThePartitionToTheKeyBasedPartitioner() {
        givenTheBrokerAcknowledges();

        publisher.publish(givenARequest());

        assertThat(whenTheRecordIsCaptured().partition()).isNull();
    }

    @Test
    void carriesTheRequestAsTheRecordValue() {
        givenTheBrokerAcknowledges();
        final var request = givenARequest();

        publisher.publish(request);

        assertThat(whenTheRecordIsCaptured().value()).isEqualTo(request);
    }

    @Test
    void returnsAPendingOutcomeWhileTheBrokerHasNotAnswered() {
        when(template.send(any(ProducerRecord.class))).thenReturn(new CompletableFuture<>());

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isNotDone();
    }

    @Test
    void acceptsTheRequestOnceTheBrokerAcknowledges() {
        givenTheBrokerAcknowledges();

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isCompletedWithValue(new PublishOutcome.Accepted());
    }

    @Test
    void asksTheCallerToRetryWhenDeliveryExpires() {
        givenTheSendFailsWith(new TimeoutException("Expiring 1 record(s) for inference.jobs-3"));

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isCompletedWithValue(new PublishOutcome.Unavailable(RETRY_AFTER));
    }

    @Test
    void looksPastTheWrapperKafkaTemplateAddsWhenClassifyingAFailure() {
        givenTheSendFailsWith(new KafkaProducerException(new ProducerRecord<>(InferenceTopics.JOBS, JOB_ID, null),
                "Failed to send", new TimeoutException("Expiring 1 record(s)")));

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isCompletedWithValue(new PublishOutcome.Unavailable(RETRY_AFTER));
    }

    @Test
    void looksPastTheWrapperTheSynchronousPathThrows() {
        when(template.send(any(ProducerRecord.class)))
                .thenThrow(new KafkaException("Send failed", new TimeoutException("Topic metadata not available")));

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isCompletedWithValue(new PublishOutcome.Unavailable(RETRY_AFTER));
    }

    @Test
    void asksTheCallerToRetryWhenTooFewReplicasAreInSync() {
        givenTheSendFailsWith(new NotEnoughReplicasException("fewer in-sync replicas than required"));

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isCompletedWithValue(new PublishOutcome.Unavailable(RETRY_AFTER));
    }

    @Test
    void rejectsTheRequestWhenTheRecordIsTooLargeToEverSucceed() {
        givenTheSendFailsWith(new RecordTooLargeException("The message is 2000000 bytes"));

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isCompletedWithValueMatching(
                outcome -> outcome instanceof PublishOutcome.Rejected rejected
                        && rejected.reason().contains("RecordTooLargeException"));
    }

    @Test
    void rejectsTheRequestWhenTheGatewayMayNotWriteToTheTopic() {
        givenTheSendFailsWith(new TopicAuthorizationException(InferenceTopics.JOBS));

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isCompletedWithValueMatching(PublishOutcome.Rejected.class::isInstance);
    }

    @Test
    void rejectsTheRequestWhenSerializationFailsBeforeTheSend() {
        when(template.send(any(ProducerRecord.class))).thenThrow(new SerializationException("no serializer"));

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isCompletedWithValueMatching(PublishOutcome.Rejected.class::isInstance);
    }

    @Test
    void neverCompletesTheOutcomeExceptionally() {
        givenTheSendFailsWith(new RecordTooLargeException("The message is 2000000 bytes"));

        final var actualOutcome = publisher.publish(givenARequest());

        assertThat(actualOutcome).isCompleted().isNotCompletedExceptionally();
    }

    @Test
    void rejectsANullTemplate() {
        assertThatNullPointerException().isThrownBy(() -> new KafkaJobPublisher(null, RETRY_AFTER));
    }

    @Test
    void rejectsANullRetryAfter() {
        assertThatNullPointerException().isThrownBy(() -> new KafkaJobPublisher(template, null));
    }

    @Test
    void rejectsANullRequest() {
        assertThatNullPointerException().isThrownBy(() -> publisher.publish(null));
    }

    private void givenTheBrokerAcknowledges() {
        final var metadata = new RecordMetadata(new TopicPartition(InferenceTopics.JOBS, 3), 42L, 0, 0L, 0, 0);
        when(template.send(any(ProducerRecord.class)))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(
                        new SendResult<>(invocation.getArgument(0), metadata)));
    }

    private void givenTheSendFailsWith(final Throwable cause) {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(cause));
    }

    private static InferenceRequest givenARequest() {
        return new InferenceRequest(new JobId(JOB_ID), new ModelId("llama3:8b"), "why is the sky blue?", 128,
                Instant.parse("2026-08-15T10:15:30Z"));
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, InferenceRequest> whenTheRecordIsCaptured() {
        final var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        return captor.getValue();
    }
}
