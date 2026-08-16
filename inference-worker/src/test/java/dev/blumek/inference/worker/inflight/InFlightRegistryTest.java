package dev.blumek.inference.worker.inflight;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.messaging.InferenceTopics;
import dev.blumek.inference.worker.processing.Disposition;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InFlightRegistryTest {
    private static final Instant DISPATCHED_AT = Instant.parse("2026-08-16T09:00:00Z");
    private static final Duration ONE_SECOND = Duration.ofSeconds(1);
    private static final int POLL_LIMIT = 8;
    private static final int FIRST_DELIVERY = 1;
    private static final int WORKER_THREADS = 4;
    private static final Disposition ACCEPTED = new Disposition.Accept();

    private final MutableClock clock = new MutableClock(DISPATCHED_AT);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final InFlightRegistry registry = new InFlightRegistry(POLL_LIMIT, new LockMeters(meterRegistry));

    @Test
    void countsEveryDispatchedRecordAsInFlight() {
        registry.track(givenAcquired(0, 0L));
        registry.track(givenAcquired(0, 1L));

        assertThat(registry.size()).isEqualTo(2);
    }

    @Test
    void keepsRecordsSharingAnOffsetAcrossPartitionsApart() {
        final var onOnePartition = givenAcquired(0, 7L);
        final var onAnotherPartition = givenAcquired(5, 7L);
        registry.track(onOnePartition);
        registry.track(onAnotherPartition);

        whenCompleted(onOnePartition);
        registry.drainCompleted();

        assertThat(registry.abandonAll()).containsExactly(onAnotherPartition);
    }

    @Test
    void handsBackEveryCompletionAWorkerEnqueued() {
        final var inFlight = givenAcquired(0, 0L);
        registry.track(inFlight);

        whenCompleted(inFlight);

        assertThat(registry.drainCompleted()).containsExactly(new Completion(inFlight.ref(), ACCEPTED));
    }

    @Test
    void forgetsARecordOnceItsCompletionIsDrained() {
        final var inFlight = givenAcquired(0, 0L);
        registry.track(inFlight);
        whenCompleted(inFlight);

        registry.drainCompleted();

        assertThat(registry.size()).isZero();
    }

    @Test
    void drainsNothingWhileTheWorkerIsStillBusy() {
        registry.track(givenAcquired(0, 0L));

        assertThat(registry.drainCompleted()).isEmpty();
    }

    @Test
    void rejectsACompletionForARecordItIsNotTracking() {
        registry.complete(new Completion(givenRef(0, 0L), ACCEPTED));

        assertThatThrownBy(registry::drainCompleted)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("untracked");
    }

    @Test
    void handsBackTheEntryOfARecordItIsTracking() {
        final var inFlight = givenAcquired(0, 0L);
        registry.track(inFlight);

        assertThat(registry.tracking(inFlight.ref())).contains(inFlight);
    }

    @Test
    void tracksNothingForARecordItHasNeverSeen() {
        assertThat(registry.tracking(givenRef(0, 0L))).isEmpty();
    }

    @Test
    void forgetsARecordWhoseLockWasGivenUp() {
        final var inFlight = givenAcquired(0, 0L);
        registry.track(inFlight);

        registry.abandon(inFlight.ref());

        assertThat(registry.size()).isZero();
    }

    @Test
    void discardsTheLateCompletionOfARecordWhoseLockWasGivenUp() {
        final var inFlight = givenAcquired(0, 0L);
        registry.track(inFlight);
        registry.abandon(inFlight.ref());

        whenCompleted(inFlight);

        assertThat(registry.drainCompleted()).isEmpty();
    }

    @Test
    void stillRejectsASecondCompletionOfAnAbandonedRecord() {
        final var inFlight = givenAcquired(0, 0L);
        registry.track(inFlight);
        registry.abandon(inFlight.ref());
        whenCompleted(inFlight);
        registry.drainCompleted();

        whenCompleted(inFlight);

        assertThatThrownBy(registry::drainCompleted).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void wakesTheLoopAsSoonAsAWorkerFinishes() throws Exception {
        final var inFlight = givenAcquired(0, 0L);
        registry.track(inFlight);
        givenCompletedAfter(inFlight, Duration.ofMillis(20));

        assertThat(registry.awaitCompleted(Duration.ofSeconds(5))).hasSize(1);
    }

    @Test
    void waitsNoLongerThanAskedWhenNoWorkerFinishes() {
        registry.track(givenAcquired(0, 0L));

        assertThat(registry.awaitCompleted(Duration.ofMillis(20))).isEmpty();
    }

    private void givenCompletedAfter(final InFlight inFlight, final Duration delay) {
        Thread.ofVirtual().start(() -> {
            try {
                Thread.sleep(delay);
                whenCompleted(inFlight);
            } catch (final InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
    }

    @Test
    void acceptsCompletionsFromEveryWorkerThreadAtOnce() throws Exception {
        final var dispatched = givenDispatched(POLL_LIMIT);

        whenCompletedConcurrently(dispatched);

        assertThat(registry.drainCompleted()).hasSize(POLL_LIMIT);
    }

    private void whenCompletedConcurrently(final List<InFlight> dispatched) throws Exception {
        try (final var workers = Executors.newFixedThreadPool(WORKER_THREADS)) {
            workers.invokeAll(dispatched.stream()
                    .<Callable<Boolean>>map(inFlight -> () -> {
                        whenCompleted(inFlight);
                        return true;
                    })
                    .toList());
        }
    }

    @Test
    void countsHowOftenAJobWasRenewedBeforeItFinished() {
        final var inFlight = givenAcquired(0, 0L);
        registry.track(inFlight.refreshed(DISPATCHED_AT).refreshed(DISPATCHED_AT));
        whenCompleted(inFlight);

        registry.drainCompleted();

        assertThat(meterRegistry.summary(LockMeters.RENEWALS_PER_JOB).max()).isEqualTo(2.0);
    }

    @Test
    void countsTheLateCompletionOfARecordWhoseLockWasGivenUp() {
        final var inFlight = givenAcquired(0, 0L);
        registry.track(inFlight);
        registry.abandon(inFlight.ref());
        whenCompleted(inFlight);

        registry.drainCompleted();

        assertThat(meterRegistry.counter(LockMeters.LATE_COMPLETIONS).count()).isEqualTo(1.0);
    }

    @Test
    void keepsTrackingARecordRedeliveredAfterItsLockWasGivenUp() {
        final var lost = givenAcquired(0, 0L);
        registry.track(lost);
        registry.abandon(lost.ref());
        registry.track(givenAcquired(0, 0L));

        whenCompleted(lost);
        registry.drainCompleted();

        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void handsBackEveryTrackedRecordOnShutdown() {
        final var dispatched = givenDispatched(2);

        assertThat(registry.abandonAll()).containsExactlyInAnyOrderElementsOf(dispatched);
    }

    @Test
    void emptiesItselfOnShutdown() {
        givenDispatched(2);

        registry.abandonAll();

        assertThat(registry.size()).isZero();
    }

    @Test
    void reportsTheOldestAcquisitionAsTheStuckWorkerSignal() {
        registry.track(givenAcquired(0, 0L));
        whenTimePasses(ONE_SECOND);
        registry.track(givenAcquired(1, 0L));

        assertThat(registry.oldestAcquiredAt()).contains(DISPATCHED_AT);
    }

    @Test
    void reportsNoAcquisitionWhenNothingIsInFlight() {
        assertThat(registry.oldestAcquiredAt()).isEmpty();
    }

    @Test
    void refusesToTrackMoreRecordsThanASingleFetchMayDeliver() {
        givenDispatched(POLL_LIMIT);

        assertThatThrownBy(() -> registry.track(givenAcquired(0, POLL_LIMIT)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("in-flight limit");
    }

    @Test
    void acceptsARefreshedRecordWhileHoldingAFullBatch() {
        final var dispatched = givenDispatched(POLL_LIMIT);
        whenTimePasses(ONE_SECOND);

        whenRenewed(dispatched);

        assertThat(registry.size()).isEqualTo(POLL_LIMIT);
    }

    private List<InFlight> givenDispatched(final int records) {
        final var dispatched = IntStream.range(0, records)
                .mapToObj(offset -> givenAcquired(0, offset))
                .toList();
        dispatched.forEach(registry::track);
        return dispatched;
    }

    private InFlight givenAcquired(final int partition, final long offset) {
        return InFlight.acquired(givenRef(partition, offset), givenRequest(), FIRST_DELIVERY, clock.instant());
    }

    private static RecordRef givenRef(final int partition, final long offset) {
        return new RecordRef(new TopicPartition(InferenceTopics.JOBS, partition), offset);
    }

    private static InferenceRequest givenRequest() {
        return new InferenceRequest(JobId.newId(), new ModelId("llama3:8b"), "why is the sky blue?", 128,
                DISPATCHED_AT);
    }

    private void whenCompleted(final InFlight inFlight) {
        registry.complete(new Completion(inFlight.ref(), ACCEPTED));
    }

    private void whenRenewed(final List<InFlight> due) {
        due.forEach(inFlight -> registry.track(inFlight.refreshed(clock.instant())));
    }

    private void whenTimePasses(final Duration duration) {
        clock.advanceBy(duration);
    }
}
