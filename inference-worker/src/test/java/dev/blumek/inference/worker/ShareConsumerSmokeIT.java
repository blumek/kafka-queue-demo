package dev.blumek.inference.worker;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.messaging.InferenceSerdes;
import dev.blumek.inference.messaging.InferenceTopics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ShareConsumerSmokeIT {
    private static final String GROUP = "inference-workers";
    private static final int PARTITIONS = 1;
    private static final short REPLICAS = 1;
    private static final TopicPartition THE_ONLY_PARTITION = new TopicPartition(InferenceTopics.JOBS, 0);

    private static final Duration POLL_INTERVAL = Duration.ofMillis(500);
    private static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(30);
    private static final int BOTH_CONSUMERS = 2;
    private static final long THE_FIRST_RECORD = 0L;

    @Container
    private final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"))
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withEnv("KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer,share")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1");

    @BeforeEach
    void givenTheJobsTopicAndTheShareGroupAreConfigured() throws Exception {
        try (final var admin = givenAnAdmin()) {
            admin.createTopics(List.of(new NewTopic(InferenceTopics.JOBS, PARTITIONS, REPLICAS))).all().get();
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP, GROUP), List.of(
                    setting("share.auto.offset.reset", "earliest"),
                    setting("share.delivery.count.limit", "4"),
                    setting("share.record.lock.duration.ms", "30000")))).all().get();
        }
    }

    @Test
    void deliversAPublishedJobToAShareConsumer() {
        final var request = givenAJob();
        givenPublished(request);

        try (final var consumer = givenAShareConsumer()) {
            assertThat(whenRecordsAreDeliveredTo(consumer))
                    .singleElement()
                    .satisfies(record -> assertThat(record.key()).isEqualTo(request.jobId().value()))
                    .satisfies(record -> assertThat(record.value()).isEqualTo(request));
        }
    }

    @Test
    void handsTheOnlyPartitionToBothConsumersAtOnce() throws Exception {
        try (final var one = givenAShareConsumer(); final var another = givenAShareConsumer()) {
            assertThat(whenBothHaveJoinedTheShareGroup(one, another)).isEqualTo(BOTH_CONSUMERS);
        }
    }

    @Test
    void deliversRecordsToASecondConsumerOnASinglePartitionTopic() throws Exception {
        try (final var one = givenAShareConsumer(); final var another = givenAShareConsumer()) {
            whenBothHaveJoinedTheShareGroup(one, another);

            final var actualShare = whenJobsTrickleOntoTheTopic(one, another);

            assertThat(actualShare.byOne()).isNotEmpty();
            assertThat(actualShare.byAnother()).isNotEmpty();
        }
    }

    @Test
    void deliversEachRecordToOnlyOneOfTheConsumers() throws Exception {
        try (final var one = givenAShareConsumer(); final var another = givenAShareConsumer()) {
            whenBothHaveJoinedTheShareGroup(one, another);

            final var actualShare = whenJobsTrickleOntoTheTopic(one, another);

            assertThat(actualShare.delivered())
                    .doesNotHaveDuplicates()
                    .isSubsetOf(actualShare.published());
        }
    }

    @Test
    void withholdsTheImplicitAcknowledgementUntilTheNextPoll() {
        givenPublished(givenAJob());

        try (final var consumer = givenAShareConsumer()) {
            final var acknowledged = givenTheAcknowledgementsAreRecorded(consumer);

            whenRecordsAreDeliveredTo(consumer);

            assertThat(acknowledged).isEmpty();
            assertThat(whenPolledUntilTheAcknowledgementLands(consumer, acknowledged))
                    .containsExactly(THE_FIRST_RECORD);
        }
    }

    private ShareConsumer<String, InferenceRequest> givenAShareConsumer() {
        final var configuration = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        final var consumer = new KafkaShareConsumer<>(configuration, new StringDeserializer(),
                InferenceSerdes.requestDeserializer());
        consumer.subscribe(List.of(InferenceTopics.JOBS));
        return consumer;
    }

    private static List<Long> givenTheAcknowledgementsAreRecorded(final ShareConsumer<String, InferenceRequest> consumer) {
        final var acknowledged = new CopyOnWriteArrayList<Long>();
        consumer.setAcknowledgementCommitCallback((offsets, failure) -> offsets.values().forEach(acknowledged::addAll));
        return acknowledged;
    }

    private long whenBothHaveJoinedTheShareGroup(final ShareConsumer<String, InferenceRequest> one,
                                                 final ShareConsumer<String, InferenceRequest> another) throws Exception {
        try (final var admin = givenAnAdmin()) {
            final var deadline = Instant.now().plus(DELIVERY_TIMEOUT);
            var holders = 0L;
            do {
                one.poll(POLL_INTERVAL);
                another.poll(POLL_INTERVAL);
                holders = membersHoldingTheOnlyPartition(admin);
            } while (holders < BOTH_CONSUMERS && Instant.now().isBefore(deadline));
            return holders;
        }
    }

    private static long membersHoldingTheOnlyPartition(final Admin admin) throws Exception {
        return admin.describeShareGroups(List.of(GROUP)).all().get().get(GROUP).members().stream()
                .filter(member -> member.assignment().topicPartitions().contains(THE_ONLY_PARTITION))
                .count();
    }

    private void givenPublished(final InferenceRequest request) {
        final var configuration = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (final var producer = new KafkaProducer<>(configuration, new StringSerializer(),
                InferenceSerdes.requestSerializer())) {
            producer.send(new ProducerRecord<>(InferenceTopics.JOBS, request.jobId().value(), request));
            producer.flush();
        }
    }

    private SharedBatch whenJobsTrickleOntoTheTopic(final ShareConsumer<String, InferenceRequest> one,
                                                    final ShareConsumer<String, InferenceRequest> another) {
        final var published = new ArrayList<String>();
        final var byOne = new ArrayList<String>();
        final var byAnother = new ArrayList<String>();
        final var deadline = Instant.now().plus(DELIVERY_TIMEOUT);
        while ((byOne.isEmpty() || byAnother.isEmpty()) && Instant.now().isBefore(deadline)) {
            final var job = givenAJob();
            givenPublished(job);
            published.add(job.jobId().value());
            one.poll(POLL_INTERVAL).forEach(record -> byOne.add(record.key()));
            another.poll(POLL_INTERVAL).forEach(record -> byAnother.add(record.key()));
        }
        return new SharedBatch(published, byOne, byAnother);
    }

    private record SharedBatch(List<String> published, List<String> byOne, List<String> byAnother) {
        List<String> delivered() {
            return Stream.concat(byOne.stream(), byAnother.stream()).toList();
        }
    }

    private static List<ConsumerRecord<String, InferenceRequest>> whenRecordsAreDeliveredTo(
            final ShareConsumer<String, InferenceRequest> consumer) {
        final var delivered = new ArrayList<ConsumerRecord<String, InferenceRequest>>();
        final var deadline = Instant.now().plus(DELIVERY_TIMEOUT);
        while (delivered.isEmpty() && Instant.now().isBefore(deadline)) {
            consumer.poll(POLL_INTERVAL).forEach(delivered::add);
        }
        return delivered;
    }

    private static List<Long> whenPolledUntilTheAcknowledgementLands(final ShareConsumer<String, InferenceRequest> consumer,
                                                                    final List<Long> acknowledged) {
        final var deadline = Instant.now().plus(DELIVERY_TIMEOUT);
        while (acknowledged.isEmpty() && Instant.now().isBefore(deadline)) {
            consumer.poll(POLL_INTERVAL);
        }
        return acknowledged;
    }

    private Admin givenAnAdmin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    private static AlterConfigOp setting(final String name, final String value) {
        return new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static InferenceRequest givenAJob() {
        return new InferenceRequest(JobId.newId(), new ModelId("llama3:8b"), "why is the sky blue?", 128,
                Instant.parse("2026-08-15T10:15:30Z"));
    }
}
