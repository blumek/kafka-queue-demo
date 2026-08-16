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
import dev.blumek.inference.worker.processing.Disposition;
import dev.blumek.inference.worker.processing.DispositionClassifier;
import dev.blumek.inference.worker.processing.JobProcessor;
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
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class ShareConsumptionIT {
    private static final int PARTITIONS = 6;
    private static final short REPLICAS = 1;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(200);
    private static final Duration PATIENCE = Duration.ofSeconds(90);
    private static final Duration ROOMY_LOCK = Duration.ofSeconds(30);
    private static final Duration TIGHT_LOCK = Duration.ofSeconds(4);
    private static final Duration ENGINE_LATENCY = Duration.ofMillis(300);
    private static final int UNLIMITED_RENEWALS = Integer.MAX_VALUE;
    private static final int PER_INSTANCE_CONCURRENCY = 4;
    private static final int ONE_JOB_AT_A_TIME = 1;
    private static final int INSTANCES = 8;
    private static final int A_BATCH_OF_JOBS = 12;
    private static final int SECOND_DELIVERY = 2;
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
    private final CountDownLatch released = new CountDownLatch(1);

    @BeforeAll
    static void givenTheJobsTopicExists() throws Exception {
        try (final var admin = givenAnAdmin()) {
            admin.createTopics(List.of(new NewTopic(InferenceTopics.JOBS, PARTITIONS, REPLICAS))).all().get();
        } catch (final ExecutionException alreadyThere) {
            assertThat(alreadyThere).hasCauseInstanceOf(TopicExistsException.class);
        }
    }

    @AfterEach
    void stopEveryWorker() {
        released.countDown();
        workers.forEach(RunningWorker::stop);
        workers.forEach(RunningWorker::close);
        jobThreads.shutdownNow();
    }

    @Test
    void neverWorksOnMoreJobsAtOnceThanASingleFetchMayDeliver() throws Exception {
        givenTheGroupIsConfigured("in-flight-cap", ROOMY_LOCK);
        final var ledger = new Ledger();
        final var jobs = whenPublished(A_BATCH_OF_JOBS);
        givenAWorkerIn("in-flight-cap", PER_INSTANCE_CONCURRENCY, ledger, givenAnEngineTaking(ENGINE_LATENCY, ledger));

        awaitUntil(() -> ledger.completed().containsAll(jobs));

        assertThat(ledger.peakConcurrency()).isBetween(2, PER_INSTANCE_CONCURRENCY);
    }

    @Test
    void putsEveryInstanceOfTheGroupToWork() throws Exception {
        givenTheGroupIsConfigured("every-instance", ROOMY_LOCK);
        final var ledgers = IntStream.range(0, INSTANCES).mapToObj(_ -> new Ledger()).toList();
        ledgers.forEach(ledger -> givenAWorkerIn("every-instance", PER_INSTANCE_CONCURRENCY, ledger,
                givenAnEngineTaking(ENGINE_LATENCY, ledger)));

        awaitUntil(() -> whenJobsKeepArriving(ledgers));

        assertThat(ledgers).allSatisfy(ledger -> assertThat(ledger.completed()).isNotEmpty());
    }

    @Test
    void redeliversTheJobsOfAnInstanceThatDiedMidFlight() throws Exception {
        givenTheGroupIsConfigured("dead-instance", TIGHT_LOCK);
        final var wedged = new Ledger();
        final var job = whenPublished(1).getFirst();
        givenAWorkerIn("dead-instance", ONE_JOB_AT_A_TIME, wedged, givenAnEngineHangingOn(job, wedged));
        awaitUntil(() -> wedged.started().contains(job));

        whenTheInstanceStopsRenewingItsLocks();

        final var healthy = new Ledger();
        IntStream.range(0, PARTITIONS).forEach(_ -> givenAWorkerIn("dead-instance", ONE_JOB_AT_A_TIME, healthy,
                givenAnEngineTaking(Duration.ZERO, healthy)));
        awaitUntil(() -> healthy.completed().contains(job));
        assertThat(healthy.deliveriesOf(job)).isGreaterThanOrEqualTo(SECOND_DELIVERY);
    }

    private void whenTheInstanceStopsRenewingItsLocks() {
        workers.forEach(RunningWorker::stop);
    }

    private boolean whenJobsKeepArriving(final List<Ledger> ledgers) throws Exception {
        whenPublished(INSTANCES * 2);
        Thread.sleep(ENGINE_LATENCY);
        return ledgers.stream().allMatch(ledger -> !ledger.completed().isEmpty());
    }

    private void givenAWorkerIn(final String group,
                                final int maxPollRecords,
                                final Ledger ledger,
                                final InferenceEngine engine) {
        final var consumer = ShareConsumerFactory.jobConsumer(
                ShareConsumerFactory.consumerConfiguration(KAFKA.getBootstrapServers(), group, maxPollRecords));
        final var registry = new InFlightRegistry(Clock.systemUTC(), maxPollRecords, UNLIMITED_RENEWALS);
        final var processor = new JobProcessor(engine, givenACompletionLedger(ledger));
        final var strategy = new ShareConsumptionStrategy(consumer, processor, registry, jobThreads, Clock.systemUTC(),
                POLL_TIMEOUT);
        workers.add(new RunningWorker(consumer, strategy, Thread.ofPlatform().start(strategy::consume)));
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

    private static DispositionClassifier givenACompletionLedger(final Ledger ledger) {
        return (outcome, deliveryCount) -> {
            if (outcome instanceof EngineOutcome.Completed completed) {
                ledger.completed(completed.result().jobId().value(), deliveryCount);
            }
            return new Disposition.Accept();
        };
    }

    private InferenceEngine givenAnEngineTaking(final Duration latency, final Ledger ledger) {
        return request -> {
            ledger.started(request.jobId().value());
            try {
                Thread.sleep(latency);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            ledger.finished();
            return new EngineOutcome.Completed(givenAResult(request));
        };
    }

    private InferenceEngine givenAnEngineHangingOn(final String jobId, final Ledger ledger) {
        return request -> {
            ledger.started(request.jobId().value());
            if (request.jobId().value().equals(jobId)) {
                awaitRelease();
            }
            ledger.finished();
            return new EngineOutcome.Completed(givenAResult(request));
        };
    }

    private void awaitRelease() {
        try {
            released.await();
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class Ledger {
        private final Set<String> started = ConcurrentHashMap.newKeySet();
        private final Map<String, Integer> deliveries = new ConcurrentHashMap<>();
        private final AtomicInteger running = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();

        void started(final String jobId) {
            started.add(jobId);
            peak.accumulateAndGet(running.incrementAndGet(), Math::max);
        }

        void finished() {
            running.decrementAndGet();
        }

        void completed(final String jobId, final int deliveryCount) {
            started.add(jobId);
            deliveries.put(jobId, deliveryCount);
        }

        Set<String> started() {
            return Set.copyOf(started);
        }

        Set<String> completed() {
            return Set.copyOf(deliveries.keySet());
        }

        int deliveriesOf(final String jobId) {
            return deliveries.getOrDefault(jobId, 0);
        }

        int peakConcurrency() {
            return peak.get();
        }
    }

    private static void givenTheGroupIsConfigured(final String group, final Duration lockDuration) throws Exception {
        try (final var admin = givenAnAdmin()) {
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP, group), List.of(
                    setting("share.auto.offset.reset", "earliest"),
                    setting("share.delivery.count.limit", "5"),
                    setting("share.record.lock.duration.ms", String.valueOf(lockDuration.toMillis()))))).all().get();
        }
    }

    private static AlterConfigOp setting(final String name, final String value) {
        return new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOp.OpType.SET);
    }

    private static List<String> whenPublished(final int jobs) {
        final var configuration = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (final var producer = new KafkaProducer<>(configuration, new StringSerializer(),
                InferenceSerdes.requestSerializer())) {
            final var published = IntStream.range(0, jobs).mapToObj(_ -> givenAJob()).toList();
            published.forEach(request ->
                    producer.send(new ProducerRecord<>(InferenceTopics.JOBS, request.jobId().value(), request)));
            producer.flush();
            return published.stream().map(request -> request.jobId().value()).toList();
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
                ENGINE_LATENCY, SUBMITTED_AT);
    }

    private static void awaitUntil(final Condition condition) throws Exception {
        final var deadline = Instant.now().plus(PATIENCE);
        while (!condition.isMet()) {
            assertThat(Instant.now()).as("the workers never got there").isBefore(deadline);
            Thread.sleep(Duration.ofMillis(20));
        }
    }

    @FunctionalInterface
    private interface Condition {
        boolean isMet() throws Exception;
    }
}
