package dev.blumek.inference.worker.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.InferenceResult;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.domain.port.EngineOutcome;
import dev.blumek.inference.domain.port.InferenceEngine;
import dev.blumek.inference.messaging.InferenceSerdes;
import dev.blumek.inference.messaging.InferenceTopics;
import dev.blumek.inference.worker.inflight.InFlightRegistry;
import dev.blumek.inference.worker.inflight.LockMeters;
import dev.blumek.inference.worker.inflight.RenewalPolicy;
import dev.blumek.inference.worker.processing.Disposition;
import dev.blumek.inference.worker.processing.DispositionClassifier;
import dev.blumek.inference.worker.processing.JobProcessor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class LockRenewalIT {
    private static final int PARTITIONS = 6;
    private static final short REPLICAS = 1;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration PATIENCE = Duration.ofSeconds(90);
    private static final Duration TIGHT_LOCK = Duration.ofSeconds(2);
    private static final Duration THREE_TIMES_THE_LOCK = TIGHT_LOCK.multipliedBy(3);
    private static final Duration ROOMY_BUDGET = Duration.ofMinutes(1);
    private static final Duration BUDGET_BELOW_ONE_RENEWAL = Duration.ofMillis(500);
    private static final int ONE_JOB_AT_A_TIME = 1;
    private static final int ONE_INVOCATION = 1;
    private static final int A_SECOND_INVOCATION = 2;
    private static final int SEVERAL_RENEWALS = 2;
    private static final int TWO_JOBS = 2;
    private static final Instant SUBMITTED_AT = Instant.parse("2026-08-16T09:00:00Z");

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"))
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
            .withEnv("KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS", "classic,consumer,share")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
            .withEnv("KAFKA_GROUP_SHARE_MIN_RECORD_LOCK_DURATION_MS", "1000");

    private final List<RunningWorker> workers = new ArrayList<>();
    private final ExecutorService jobThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final Map<String, AtomicInteger> invocations = new ConcurrentHashMap<>();
    private final Set<String> completed = ConcurrentHashMap.newKeySet();

    @BeforeEach
    void givenAnEmptyJobsTopic() throws Exception {
        try (final var admin = givenAnAdmin()) {
            whenTheJobsTopicIsDeleted(admin);
            awaitUntil(() -> !admin.listTopics().names().get().contains(InferenceTopics.JOBS));
            admin.createTopics(List.of(new NewTopic(InferenceTopics.JOBS, PARTITIONS, REPLICAS))).all().get();
        }
    }

    private static void whenTheJobsTopicIsDeleted(final Admin admin) throws Exception {
        try {
            admin.deleteTopics(List.of(InferenceTopics.JOBS)).all().get();
        } catch (final ExecutionException notThereYet) {
            assertThat(notThereYet).hasCauseInstanceOf(UnknownTopicOrPartitionException.class);
        }
    }

    @AfterEach
    void stopEveryWorker() {
        workers.forEach(RunningWorker::stop);
        workers.forEach(RunningWorker::close);
        jobThreads.shutdownNow();
    }

    @Test
    void runsAJobOutlastingItsLockExactlyOnceWhileTheLockIsRenewed() throws Exception {
        givenTheGroupIsConfigured("renewed-locks");
        final var job = whenPublished();
        givenAWorkerIn("renewed-locks", RenewalPolicy.over(ROOMY_BUDGET));

        awaitUntil(() -> completed.contains(job));

        assertThat(invocationsOf(job)).isEqualTo(ONE_INVOCATION);
    }

    @Test
    void renewsTheLockOfALongJobSeveralTimesBeforeItFinishes() throws Exception {
        givenTheGroupIsConfigured("renewal-counter");
        whenPublished();
        givenAWorkerIn("renewal-counter", RenewalPolicy.over(ROOMY_BUDGET));

        awaitUntil(() -> renewals() >= SEVERAL_RENEWALS);
    }

    @Test
    void cannotSaveTheLockOfAJobWaitingBehindALongOneBecauseItHasNotBeenPolledYet() throws Exception {
        givenTheGroupIsConfigured("prefetched-locks");
        final var first = whenPublished();
        final var second = whenPublished();
        givenAWorkerIn("prefetched-locks", RenewalPolicy.over(ROOMY_BUDGET));

        awaitUntil(() -> completed.contains(first) && completed.contains(second));

        assertThat(invocationsOf(first) + invocationsOf(second)).isGreaterThan(TWO_JOBS);
    }

    @Test
    void runsAJobOutlastingItsLockAgainWhenTheLockIsNotRenewed() throws Exception {
        givenTheGroupIsConfigured("unrenewed-locks");
        final var job = whenPublished();
        givenAWorkerIn("unrenewed-locks", RenewalPolicy.over(BUDGET_BELOW_ONE_RENEWAL));

        awaitUntil(() -> invocationsOf(job) >= A_SECOND_INVOCATION);
    }

    private double renewals() {
        return meterRegistry.counter(LockMeters.RENEWED).count();
    }

    private int invocationsOf(final String jobId) {
        return invocations.getOrDefault(jobId, new AtomicInteger()).get();
    }

    private void givenAWorkerIn(final String group, final RenewalPolicy renewalPolicy) {
        final var consumer = ShareConsumerFactory.jobConsumer(
                ShareConsumerFactory.consumerConfiguration(KAFKA.getBootstrapServers(), group, ONE_JOB_AT_A_TIME));
        final var meters = new LockMeters(meterRegistry);
        final var registry = new InFlightRegistry(ONE_JOB_AT_A_TIME, meters);
        final var processor = new JobProcessor(givenAnEngineTaking(THREE_TIMES_THE_LOCK), givenACompletionLedger());
        final var strategy = new ShareConsumptionStrategy(consumer, processor, registry, jobThreads, Clock.systemUTC(),
                POLL_TIMEOUT, renewalPolicy, meters);
        workers.add(new RunningWorker(consumer, strategy, Thread.ofPlatform().start(strategy::consume)));
    }

    private DispositionClassifier givenACompletionLedger() {
        return (outcome, deliveryCount) -> {
            if (outcome instanceof EngineOutcome.Completed finished) {
                completed.add(finished.result().jobId().value());
            }
            return new Disposition.Accept();
        };
    }

    private InferenceEngine givenAnEngineTaking(final Duration latency) {
        return request -> {
            invocations.computeIfAbsent(request.jobId().value(), _ -> new AtomicInteger()).incrementAndGet();
            sleep(latency);
            return new EngineOutcome.Completed(givenAResult(request));
        };
    }

    private record RunningWorker(ShareConsumer<String, InferenceRequest> consumer,
                                 ShareConsumptionStrategy strategy,
                                 Thread loop) {

        void stop() {
            strategy.stop();
            try {
                loop.join(PATIENCE.toMillis());
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        void close() {
            consumer.close(Duration.ofSeconds(5));
        }
    }

    private static void givenTheGroupIsConfigured(final String group) throws Exception {
        try (final var admin = givenAnAdmin()) {
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP, group), List.of(
                    setting("share.auto.offset.reset", "earliest"),
                    setting("share.delivery.count.limit", "5"),
                    setting("share.record.lock.duration.ms", String.valueOf(TIGHT_LOCK.toMillis()))))).all().get();
        }
    }

    private static AlterConfigOp setting(final String name, final String value) {
        return new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static String whenPublished() {
        final var configuration = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (final var producer = new KafkaProducer<>(configuration, new StringSerializer(),
                InferenceSerdes.requestSerializer())) {
            final var request = givenAJob();
            producer.send(new ProducerRecord<>(InferenceTopics.JOBS, request.jobId().value(), request));
            producer.flush();
            return request.jobId().value();
        }
    }

    private static Admin givenAnAdmin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()));
    }

    private static InferenceRequest givenAJob() {
        return new InferenceRequest(JobId.newId(), new ModelId("llama3:8b"), "why is the sky blue?", 128, SUBMITTED_AT);
    }

    private static InferenceResult givenAResult(final InferenceRequest request) {
        return new InferenceResult(request.jobId(), request.model(), "because of Rayleigh scattering", 5, 6,
                THREE_TIMES_THE_LOCK, SUBMITTED_AT);
    }

    private static void sleep(final Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void awaitUntil(final Condition condition) throws Exception {
        final var deadline = Instant.now().plus(PATIENCE);
        while (!condition.isMet()) {
            assertThat(Instant.now()).as("the worker never got there").isBefore(deadline);
            Thread.sleep(Duration.ofMillis(20));
        }
    }

    @FunctionalInterface
    private interface Condition {
        boolean isMet() throws Exception;
    }
}
