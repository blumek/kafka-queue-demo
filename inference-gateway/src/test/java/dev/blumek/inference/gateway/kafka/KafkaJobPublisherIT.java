package dev.blumek.inference.gateway.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.gateway.submission.JobPublisher;
import dev.blumek.inference.gateway.submission.PublishOutcome;
import dev.blumek.inference.messaging.InferenceSerdes;
import dev.blumek.inference.messaging.InferenceTopics;
import dev.blumek.inference.messaging.JobHeaders;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Import(KafkaJobPublisherIT.Containers.class)
class KafkaJobPublisherIT {
    private static final int PARTITIONS = 6;
    private static final short REPLICAS = 1;
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    private static final int SPREAD_SAMPLE = 100;

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() {
            return new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        }
    }

    @Autowired
    private JobPublisher publisher;

    @Autowired
    private KafkaContainer kafka;

    @Autowired
    private Tracer tracer;

    @BeforeEach
    void givenTheJobsTopicExists() throws Exception {
        try (final var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(InferenceTopics.JOBS, PARTITIONS, REPLICAS))).all().get();
        } catch (final ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    @Test
    void publishesARequestToTheJobsTopicKeyedByItsJobId() {
        final var request = givenARequest(JobId.newId());

        final var actualOutcome = publisher.publish(request).join();

        assertThat(actualOutcome).isEqualTo(new PublishOutcome.Accepted());
        final var actualRecords = whenTheTopicIsDrained();
        assertThat(actualRecords)
                .filteredOn(record -> record.key().equals(request.jobId().value()))
                .singleElement()
                .satisfies(record -> assertThat(record.value()).isEqualTo(request));
    }

    @Test
    void placesRequestsSharingAJobIdOnTheSamePartition() {
        final var jobId = JobId.newId();

        publisher.publish(givenARequest(jobId)).join();
        publisher.publish(givenARequest(jobId)).join();

        final var actualPartitions = whenTheTopicIsDrained().stream()
                .filter(record -> record.key().equals(jobId.value()))
                .map(ConsumerRecord::partition)
                .distinct()
                .toList();
        assertThat(actualPartitions).hasSize(1);
    }

    @Test
    void spreadsDistinctJobIdsAcrossEveryPartition() {
        final var published = IntStream.range(0, SPREAD_SAMPLE)
                .mapToObj(i -> publisher.publish(givenARequest(JobId.newId())))
                .toList();
        CompletableFuture.allOf(published.toArray(CompletableFuture[]::new)).join();

        assertThat(published).allSatisfy(outcome -> assertThat(outcome.join()).isEqualTo(new PublishOutcome.Accepted()));
        final var actualPartitions = whenTheTopicIsDrained().stream()
                .map(ConsumerRecord::partition)
                .distinct()
                .toList();
        assertThat(actualPartitions).hasSize(PARTITIONS);
    }

    @Test
    void stampsThePublishedRecordWithTheTraceItWasPublishedOn() {
        final var request = givenARequest(JobId.newId());

        final var actualTraceId = whenPublishedOnAFreshTrace(request);

        assertThat(whenTheTopicIsDrained())
                .filteredOn(record -> record.key().equals(request.jobId().value()))
                .singleElement()
                .satisfies(record -> assertThat(JobHeaders.traceparent(record.headers()))
                        .hasValueSatisfying(traceparent -> assertThat(traceparent)
                                .startsWith("00-" + actualTraceId + "-")));
    }

    @Test
    void leavesARecordPublishedOutsideAnyTraceUnstamped() {
        final var request = givenARequest(JobId.newId());

        publisher.publish(request).join();

        assertThat(whenTheTopicIsDrained())
                .filteredOn(record -> record.key().equals(request.jobId().value()))
                .singleElement()
                .satisfies(record -> assertThat(JobHeaders.traceparent(record.headers())).isEmpty());
    }

    private String whenPublishedOnAFreshTrace(final InferenceRequest request) {
        final var span = tracer.startScopedSpan("submit a job");
        try {
            publisher.publish(request).join();
            return span.context().traceId();
        } finally {
            span.end();
        }
    }

    private List<ConsumerRecord<String, InferenceRequest>> whenTheTopicIsDrained() {
        final var configuration = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "inference-gateway-publisher-it",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (final var consumer = new KafkaConsumer<>(configuration, new StringDeserializer(), InferenceSerdes.requestDeserializer())) {
            final var partitions = consumer.partitionsFor(InferenceTopics.JOBS).stream()
                    .map(partition -> new TopicPartition(partition.topic(), partition.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            final var ends = consumer.endOffsets(partitions);

            final var drained = new ArrayList<ConsumerRecord<String, InferenceRequest>>();
            final var deadline = Instant.now().plus(DRAIN_TIMEOUT);
            while (!hasReachedEnd(consumer, partitions, ends) && Instant.now().isBefore(deadline)) {
                consumer.poll(POLL_INTERVAL).forEach(drained::add);
            }
            return drained;
        }
    }

    private static boolean hasReachedEnd(final Consumer<String, InferenceRequest> consumer,
                                         final List<TopicPartition> partitions,
                                         final Map<TopicPartition, Long> ends) {
        return partitions.stream().allMatch(partition -> consumer.position(partition) >= ends.get(partition));
    }

    private static InferenceRequest givenARequest(final JobId jobId) {
        return new InferenceRequest(jobId, new ModelId("llama3:8b"), "why is the sky blue?", 128,
                Instant.parse("2026-08-15T10:15:30Z"));
    }
}
