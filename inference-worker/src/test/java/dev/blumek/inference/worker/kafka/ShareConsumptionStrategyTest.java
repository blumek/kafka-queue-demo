package dev.blumek.inference.worker.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.InferenceResult;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.domain.port.EngineOutcome;
import dev.blumek.inference.domain.port.InferenceEngine;
import dev.blumek.inference.messaging.InferenceTopics;
import dev.blumek.inference.worker.inflight.InFlightRegistry;
import dev.blumek.inference.worker.inflight.LockLoss;
import dev.blumek.inference.worker.inflight.LockMeters;
import dev.blumek.inference.worker.inflight.RecordRef;
import dev.blumek.inference.worker.inflight.RenewalPolicy;
import dev.blumek.inference.worker.processing.Disposition;
import dev.blumek.inference.worker.processing.DispositionClassifier;
import dev.blumek.inference.worker.processing.JobProcessor;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class ShareConsumptionStrategyTest {
    private static final Duration LOCK_TIMEOUT = Duration.ofMillis(200);
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(20);
    private static final Duration PATIENCE = Duration.ofSeconds(10);
    private static final Instant SUBMITTED_AT = Instant.parse("2026-08-16T09:00:00Z");
    private static final int BATCH_LIMIT = 4;
    private static final int FIRST_DELIVERY = 1;
    private static final int SECOND_DELIVERY = 2;
    private static final Duration A_SHORT_DELAY = Duration.ofSeconds(30);
    private static final Duration A_ROOMY_BUDGET = Duration.ofMinutes(5);
    private static final Duration ROOM_FOR_ONE_RENEWAL = LOCK_TIMEOUT.multipliedBy(3).dividedBy(4);

    private final FakeShareConsumer consumer = new FakeShareConsumer(LOCK_TIMEOUT);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final LockMeters meters = new LockMeters(meterRegistry);
    private final InFlightRegistry registry = new InFlightRegistry(BATCH_LIMIT, meters);
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private final ConcurrentLinkedQueue<Integer> deliveryCounts = new ConcurrentLinkedQueue<>();

    private InferenceEngine engine = request -> new EngineOutcome.Completed(givenAResult(request));
    private DispositionClassifier classifier = (outcome, deliveryCount) -> new Disposition.Accept();
    private RenewalPolicy renewalPolicy = RenewalPolicy.over(A_ROOMY_BUDGET);
    private ShareConsumptionStrategy strategy;
    private Thread loop;

    @AfterEach
    void stopTheLoop() throws Exception {
        if (strategy != null) {
            strategy.stop();
            loop.join(PATIENCE.toMillis());
        }
        workers.shutdownNow();
    }

    @Test
    void acceptsARecordItsProcessorCompleted() {
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L));

        assertThat(whenAcknowledged(givenRef(0, 0L))).containsExactly(AcknowledgeType.ACCEPT);
    }

    @Test
    void releasesARecordItsProcessorCouldNotComplete() {
        classifier = (outcome, deliveryCount) -> new Disposition.Release("the engine was unavailable");
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L));

        assertThat(whenAcknowledged(givenRef(0, 0L))).containsExactly(AcknowledgeType.RELEASE);
    }

    @Test
    void rejectsARecordOnlyOnceItsDeadLetterHasBeenWritten() {
        classifier = (outcome, deliveryCount) -> new Disposition.RejectAfterDlq("the prompt was malformed");
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L));

        assertThat(whenAcknowledged(givenRef(0, 0L))).containsExactly(AcknowledgeType.REJECT);
    }

    @Test
    void acceptsARecordItHandedToTheDelayTopic() {
        classifier = (outcome, deliveryCount) -> new Disposition.RetryViaDelayTopic(A_SHORT_DELAY);
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L));

        assertThat(whenAcknowledged(givenRef(0, 0L))).containsExactly(AcknowledgeType.ACCEPT);
    }

    @Test
    void releasesARecordWhoseProcessingBlewUp() {
        engine = _ -> {
            throw new IllegalStateException("the engine client was not configured");
        };
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L));

        assertThat(whenAcknowledged(givenRef(0, 0L))).containsExactly(AcknowledgeType.RELEASE);
    }

    @Test
    void tellsTheProcessorHowOftenTheRecordHasBeenDelivered() {
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L, SECOND_DELIVERY));

        whenAcknowledged(givenRef(0, 0L));
        assertThat(deliveryCounts).containsExactly(SECOND_DELIVERY);
    }

    @Test
    void renewsTheLockOfARecordItIsStillProcessing() {
        final var stillRunning = givenTheEngineBlocks();
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L));

        awaitUntil(() -> consumer.acknowledgementsOf(givenRef(0, 0L)).contains(AcknowledgeType.RENEW));
        stillRunning.countDown();
    }

    @Test
    void acceptsTheRecordOnceTheSlowJobFinallyFinishes() {
        final var stillRunning = givenTheEngineBlocks();
        givenTheLoopIsRunning();
        whenDelivered(givenARecord(0, 0L));
        awaitUntil(() -> consumer.acknowledgementsOf(givenRef(0, 0L)).contains(AcknowledgeType.RENEW));

        stillRunning.countDown();

        assertThat(whenAcknowledged(givenRef(0, 0L))).endsWith(AcknowledgeType.ACCEPT);
    }

    @Test
    void countsEveryRenewalItIssues() {
        final var stillRunning = givenTheEngineBlocks();
        givenTheLoopIsRunning();
        whenDelivered(givenARecord(0, 0L));

        awaitUntil(() -> renewalsOf(givenRef(0, 0L)) >= 2);

        assertThat(meterRegistry.counter(LockMeters.RENEWED).count()).isGreaterThanOrEqualTo(2.0);
        stillRunning.countDown();
    }

    @Test
    void recordsHowOftenAJobWasRenewedBeforeItFinished() {
        final var stillRunning = givenTheEngineBlocks();
        givenTheLoopIsRunning();
        whenDelivered(givenARecord(0, 0L));
        awaitUntil(() -> renewalsOf(givenRef(0, 0L)) >= 2);

        stillRunning.countDown();

        whenAcknowledged(givenRef(0, 0L));
        assertThat(meterRegistry.summary(LockMeters.RENEWALS_PER_JOB).max()).isGreaterThanOrEqualTo(2.0);
    }

    @Test
    void releasesAJobThatOutranItsProcessingBudget() {
        renewalPolicy = RenewalPolicy.over(ROOM_FOR_ONE_RENEWAL);
        final var stillRunning = givenTheEngineBlocks();
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L));

        assertThat(whenAcknowledged(givenRef(0, 0L)))
                .containsExactly(AcknowledgeType.RENEW, AcknowledgeType.RELEASE);
        stillRunning.countDown();
    }

    @Test
    void countsTheLockOfAJobThatOutranItsBudgetAsExpired() {
        renewalPolicy = RenewalPolicy.over(ROOM_FOR_ONE_RENEWAL);
        final var stillRunning = givenTheEngineBlocks();
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L));

        whenAcknowledged(givenRef(0, 0L));
        assertThat(whenExpired(LockLoss.RENEWALS_EXHAUSTED)).isEqualTo(1.0);
        stillRunning.countDown();
    }

    @Test
    void letsAWedgedJobFinishOnItsOwnRatherThanInterruptingIt() {
        renewalPolicy = RenewalPolicy.over(ROOM_FOR_ONE_RENEWAL);
        final var stillRunning = givenTheEngineBlocks();
        final var finished = givenTheEngineCountsWhatItFinished();
        givenTheLoopIsRunning();
        whenDelivered(givenARecord(0, 0L));
        whenAcknowledged(givenRef(0, 0L));

        stillRunning.countDown();

        awaitUntil(() -> finished.get() == 1);
    }

    @Test
    void keepsPollingAfterGivingUpOnAWedgedJob() {
        renewalPolicy = RenewalPolicy.over(ROOM_FOR_ONE_RENEWAL);
        final var stillRunning = givenTheEngineBlocks();
        givenTheLoopIsRunning();
        whenDelivered(givenARecord(0, 0L));
        whenAcknowledged(givenRef(0, 0L));
        stillRunning.countDown();

        whenDelivered(givenARecord(1, 0L));

        assertThat(whenAcknowledged(givenRef(1, 0L))).containsExactly(AcknowledgeType.ACCEPT);
    }

    @Test
    void countsALockLostMidFlightAsExpired() {
        consumer.loseTheLockOf(givenRef(0, 0L));
        givenTheLoopIsRunning();

        whenDelivered(givenARecord(0, 0L));

        awaitUntil(() -> whenExpired(LockLoss.ACKNOWLEDGEMENT_REFUSED) == 1.0);
    }

    @Test
    void runsARecordOnlyOnceHoweverOftenRenewalRedeliversIt() {
        final var stillRunning = givenTheEngineBlocks();
        final var started = givenTheEngineCountsItsCalls();
        givenTheLoopIsRunning();
        whenDelivered(givenARecord(0, 0L));

        awaitUntil(() -> renewalsOf(givenRef(0, 0L)) >= 3);

        assertThat(started).hasValue(1);
        stillRunning.countDown();
    }

    @Test
    void worksOnEveryRecordOfABatchAtOnce() {
        final var stillRunning = givenTheEngineBlocks();
        final var started = givenTheEngineCountsItsCalls();
        givenTheLoopIsRunning();

        whenDelivered(givenABatchOf(BATCH_LIMIT));

        awaitUntil(() -> started.get() == BATCH_LIMIT);
        stillRunning.countDown();
    }

    @Test
    void acknowledgesEveryRecordOfABatchBeforeAskingForAnother() {
        givenTheLoopIsRunning();

        whenDelivered(givenABatchOf(BATCH_LIMIT));

        awaitUntil(() -> consumer.acknowledgements().size() == BATCH_LIMIT);
        assertThat(consumer.acknowledgements()).extracting(FakeShareConsumer.Acknowledgement::type)
                .containsOnly(AcknowledgeType.ACCEPT);
    }

    @Test
    void keepsPollingAfterARecordWhoseLockWasLostMidFlight() {
        consumer.loseTheLockOf(givenRef(0, 0L));
        givenTheLoopIsRunning();
        whenDelivered(givenARecord(0, 0L));

        whenDelivered(givenARecord(1, 0L));

        assertThat(whenAcknowledged(givenRef(1, 0L))).containsExactly(AcknowledgeType.ACCEPT);
    }

    @Test
    void subscribesToTheJobsTopic() {
        givenTheLoopIsRunning();

        awaitUntil(() -> consumer.subscription().contains(InferenceTopics.JOBS));
    }

    private void givenTheLoopIsRunning() {
        strategy = new ShareConsumptionStrategy(consumer, givenAProcessor(), registry, workers, Clock.systemUTC(),
                POLL_TIMEOUT, renewalPolicy, meters);
        loop = Thread.ofPlatform().name("test-poll-loop").start(strategy::consume);
    }

    private JobProcessor givenAProcessor() {
        return new JobProcessor(request -> engine.run(request), (outcome, deliveryCount) -> {
            deliveryCounts.add(deliveryCount);
            return classifier.classify(outcome, deliveryCount);
        });
    }

    private CountDownLatch givenTheEngineBlocks() {
        final var stillRunning = new CountDownLatch(1);
        final var completing = engine;
        engine = request -> {
            await(stillRunning);
            return completing.run(request);
        };
        return stillRunning;
    }

    private AtomicInteger givenTheEngineCountsWhatItFinished() {
        final var finished = new AtomicInteger();
        final var counted = engine;
        engine = request -> {
            final var outcome = counted.run(request);
            finished.incrementAndGet();
            return outcome;
        };
        return finished;
    }

    private double whenExpired(final LockLoss loss) {
        return meterRegistry.counter(LockMeters.EXPIRED, LockMeters.REASON, loss.tag()).count();
    }

    private AtomicInteger givenTheEngineCountsItsCalls() {
        final var started = new AtomicInteger();
        final var counted = engine;
        engine = request -> {
            started.incrementAndGet();
            return counted.run(request);
        };
        return started;
    }

    private void whenDelivered(final ConsumerRecord<String, InferenceRequest> record) {
        whenDelivered(List.of(record));
    }

    private void whenDelivered(final List<ConsumerRecord<String, InferenceRequest>> batch) {
        consumer.deliver(batch);
    }

    private List<AcknowledgeType> whenAcknowledged(final RecordRef ref) {
        awaitUntil(() -> settled(ref));
        return consumer.acknowledgementsOf(ref);
    }

    private boolean settled(final RecordRef ref) {
        return consumer.acknowledgementsOf(ref).stream().anyMatch(type -> type != AcknowledgeType.RENEW);
    }

    private long renewalsOf(final RecordRef ref) {
        return consumer.acknowledgementsOf(ref).stream().filter(AcknowledgeType.RENEW::equals).count();
    }

    private static void awaitUntil(final BooleanSupplier condition) {
        final var deadline = Instant.now().plus(PATIENCE);
        while (!condition.getAsBoolean()) {
            assertThat(Instant.now()).as("the loop never got there").isBefore(deadline);
            sleepBriefly();
        }
    }

    private static void sleepBriefly() {
        try {
            Thread.sleep(Duration.ofMillis(1));
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void await(final CountDownLatch latch) {
        try {
            latch.await();
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static List<ConsumerRecord<String, InferenceRequest>> givenABatchOf(final int records) {
        return IntStream.range(0, records)
                .mapToObj(offset -> givenARecord(0, offset))
                .toList();
    }

    private static ConsumerRecord<String, InferenceRequest> givenARecord(final int partition, final long offset) {
        return givenARecord(partition, offset, FIRST_DELIVERY);
    }

    private static ConsumerRecord<String, InferenceRequest> givenARecord(final int partition,
                                                                        final long offset,
                                                                        final int deliveryCount) {
        final var request = givenARequest();
        return new ConsumerRecord<>(InferenceTopics.JOBS, partition, offset, SUBMITTED_AT.toEpochMilli(),
                TimestampType.CREATE_TIME, request.jobId().value().length(), 0, request.jobId().value(), request,
                new RecordHeaders(), Optional.empty(), Optional.of((short) deliveryCount));
    }

    private static RecordRef givenRef(final int partition, final long offset) {
        return new RecordRef(new TopicPartition(InferenceTopics.JOBS, partition), offset);
    }

    private static InferenceRequest givenARequest() {
        return new InferenceRequest(JobId.newId(), new ModelId("llama3:8b"), "why is the sky blue?", 128, SUBMITTED_AT);
    }

    private static InferenceResult givenAResult(final InferenceRequest request) {
        return new InferenceResult(request.jobId(), request.model(), "because of Rayleigh scattering", 5, 6,
                Duration.ofMillis(120), SUBMITTED_AT);
    }
}
