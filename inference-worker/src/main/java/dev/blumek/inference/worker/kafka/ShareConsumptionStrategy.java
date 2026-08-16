package dev.blumek.inference.worker.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.messaging.InferenceTopics;
import dev.blumek.inference.worker.inflight.Completion;
import dev.blumek.inference.worker.inflight.InFlight;
import dev.blumek.inference.worker.inflight.InFlightRegistry;
import dev.blumek.inference.worker.inflight.LockLoss;
import dev.blumek.inference.worker.inflight.LockMeters;
import dev.blumek.inference.worker.inflight.RecordRef;
import dev.blumek.inference.worker.inflight.RenewalPolicy;
import dev.blumek.inference.worker.inflight.RenewalSchedule;
import dev.blumek.inference.worker.processing.Disposition;
import dev.blumek.inference.worker.processing.JobProcessor;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.InvalidRecordStateException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

public final class ShareConsumptionStrategy implements ConsumptionStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(ShareConsumptionStrategy.class);
    private static final Duration ASSUMED_LOCK_TIMEOUT = Duration.ofSeconds(30);

    private final ShareConsumer<String, InferenceRequest> consumer;
    private final JobProcessor processor;
    private final InFlightRegistry registry;
    private final Executor workers;
    private final Clock clock;
    private final Duration pollTimeout;
    private final RenewalPolicy renewalPolicy;
    private final LockMeters meters;
    private final Map<RecordRef, Disposition> settled = new HashMap<>();

    private RenewalSchedule schedule;
    private volatile boolean running;

    public ShareConsumptionStrategy(final ShareConsumer<String, InferenceRequest> consumer,
                                    final JobProcessor processor,
                                    final InFlightRegistry registry,
                                    final Executor workers,
                                    final Clock clock,
                                    final Duration pollTimeout,
                                    final RenewalPolicy renewalPolicy,
                                    final LockMeters meters) {
        this.consumer = consumer;
        this.processor = processor;
        this.registry = registry;
        this.workers = workers;
        this.clock = clock;
        this.pollTimeout = pollTimeout;
        this.renewalPolicy = renewalPolicy;
        this.meters = meters;
        this.schedule = renewalPolicy.against(ASSUMED_LOCK_TIMEOUT);
    }

    @Override
    public void consume() {
        running = true;
        consumer.setAcknowledgementCommitCallback(this::onAcknowledgementsCommitted);
        consumer.subscribe(List.of(InferenceTopics.JOBS));
        try {
            while (running) {
                final var delivered = poll();
                dispatchNewlyAcquired(delivered);
                awaitCompletionsUntilRenewalIsDue(delivered);
                acknowledgeAll(delivered);
            }
        } catch (final WakeupException stopping) {
            LOG.info("Poll loop stopping with {} records still in flight", registry.size());
        }
    }

    private void onAcknowledgementsCommitted(final Map<TopicIdPartition, Set<Long>> offsets, final Exception failure) {
        if (failure == null) {
            return;
        }
        LOG.warn("Acknowledgements for {} did not land; giving up their locks", offsets, failure);
        offsets.forEach((partition, offsetsOnPartition) ->
                offsetsOnPartition.forEach(offset -> giveUp(new RecordRef(partition.topicPartition(), offset))));
    }

    private void giveUp(final RecordRef ref) {
        registry.abandon(ref);
        settled.remove(ref);
        meters.expired(LockLoss.ACKNOWLEDGEMENT_LOST);
    }

    private List<ConsumerRecord<String, InferenceRequest>> poll() {
        final var records = consumer.poll(pollTimeout);
        consumer.acquisitionLockTimeoutMs()
                .map(Duration::ofMillis)
                .ifPresent(this::rescheduleRenewals);
        final var delivered = new ArrayList<ConsumerRecord<String, InferenceRequest>>();
        records.forEach(delivered::add);
        return delivered;
    }

    private void rescheduleRenewals(final Duration lockTimeout) {
        final var rescheduled = renewalPolicy.against(lockTimeout);
        if (rescheduled.equals(schedule)) {
            return;
        }
        LOG.info("Renewing acquisition locks every {} and at most {} times against a lock lasting {}",
                rescheduled.renewAfter(), rescheduled.maxRenewals(), lockTimeout);
        schedule = rescheduled;
    }

    private void dispatchNewlyAcquired(final List<ConsumerRecord<String, InferenceRequest>> delivered) {
        delivered.stream()
                .filter(record -> !settled.containsKey(RecordRef.of(record)))
                .filter(record -> registry.tracking(RecordRef.of(record)).isEmpty())
                .forEach(this::dispatch);
    }

    private void dispatch(final ConsumerRecord<String, InferenceRequest> record) {
        final var inFlight = InFlight.of(record, clock.instant());
        registry.track(inFlight);
        workers.execute(() -> registry.complete(new Completion(inFlight.ref(), safely(inFlight))));
    }

    private Disposition safely(final InFlight inFlight) {
        try {
            return processor.process(inFlight.request(), inFlight.deliveryCount());
        } catch (final RuntimeException defect) {
            LOG.error("Job {} failed unexpectedly and will be released for redelivery",
                    inFlight.request().jobId().value(), defect);
            return new Disposition.Release(defect.toString());
        }
    }

    private void awaitCompletionsUntilRenewalIsDue(final List<ConsumerRecord<String, InferenceRequest>> delivered) {
        final var deadline = renewalDeadline(delivered);
        while (running && anyStillRunning(delivered)) {
            final var untilDeadline = Duration.between(clock.instant(), deadline);
            if (untilDeadline.isNegative() || untilDeadline.isZero()) {
                return;
            }
            settle(registry.awaitCompleted(shorterOf(untilDeadline, pollTimeout)));
        }
    }

    private Instant renewalDeadline(final List<ConsumerRecord<String, InferenceRequest>> delivered) {
        return trackedAmong(delivered)
                .map(schedule::dueAt)
                .min(Comparator.naturalOrder())
                .orElseGet(clock::instant);
    }

    private boolean anyStillRunning(final List<ConsumerRecord<String, InferenceRequest>> delivered) {
        return trackedAmong(delivered).findAny().isPresent();
    }

    private Stream<InFlight> trackedAmong(final List<ConsumerRecord<String, InferenceRequest>> delivered) {
        return delivered.stream()
                .map(RecordRef::of)
                .filter(ref -> !settled.containsKey(ref))
                .map(registry::tracking)
                .flatMap(Optional::stream);
    }

    private static Duration shorterOf(final Duration one, final Duration another) {
        return one.compareTo(another) <= 0 ? one : another;
    }

    private void settle(final List<Completion> completions) {
        completions.forEach(completion -> settled.put(completion.ref(), completion.disposition()));
    }

    private void acknowledgeAll(final List<ConsumerRecord<String, InferenceRequest>> delivered) {
        settle(registry.drainCompleted());
        delivered.forEach(this::settleOrRenew);
    }

    private void settleOrRenew(final ConsumerRecord<String, InferenceRequest> record) {
        final var ref = RecordRef.of(record);
        final var disposition = settled.remove(ref);
        if (disposition != null) {
            acknowledgeSafely(record, ref, acknowledgeType(disposition));
            return;
        }
        registry.tracking(ref).ifPresentOrElse(
                inFlight -> renewOrGiveUp(record, inFlight),
                () -> acknowledgeSafely(record, ref, AcknowledgeType.RELEASE));
    }

    private boolean acknowledgeSafely(final ConsumerRecord<String, InferenceRequest> record,
                                      final RecordRef ref,
                                      final AcknowledgeType type) {
        try {
            consumer.acknowledge(record, type);
            return true;
        } catch (final IllegalStateException | InvalidRecordStateException lockLost) {
            LOG.warn("Acknowledging {} as {} was refused; the record is already on its way to another consumer",
                    ref, type, lockLost);
            registry.abandon(ref);
            meters.expired(LockLoss.ACKNOWLEDGEMENT_REFUSED);
            return false;
        }
    }

    private static AcknowledgeType acknowledgeType(final Disposition disposition) {
        return switch (disposition) {
            case Disposition.Accept _             -> AcknowledgeType.ACCEPT;
            case Disposition.Release _            -> AcknowledgeType.RELEASE;
            case Disposition.RejectAfterDlq _     -> AcknowledgeType.REJECT;
            case Disposition.RetryViaDelayTopic _ -> AcknowledgeType.ACCEPT;
        };
    }

    private void renewOrGiveUp(final ConsumerRecord<String, InferenceRequest> record, final InFlight inFlight) {
        if (schedule.isExhausted(inFlight)) {
            giveUpOn(record, inFlight);
            return;
        }
        renew(record, inFlight);
    }

    private void renew(final ConsumerRecord<String, InferenceRequest> record, final InFlight inFlight) {
        if (acknowledgeSafely(record, inFlight.ref(), AcknowledgeType.RENEW)) {
            registry.track(inFlight.refreshed(clock.instant()));
            meters.renewed();
        }
    }

    private void giveUpOn(final ConsumerRecord<String, InferenceRequest> record, final InFlight inFlight) {
        LOG.warn("Job {} has held its lock for {} across {} renewals without finishing; "
                        + "releasing it for another worker while it runs on here",
                inFlight.request().jobId().value(), inFlight.heldSince(clock.instant()), inFlight.renewals());
        if (acknowledgeSafely(record, inFlight.ref(), AcknowledgeType.RELEASE)) {
            registry.abandon(inFlight.ref());
            meters.expired(LockLoss.RENEWALS_EXHAUSTED);
        }
    }

    @Override
    public void stop() {
        running = false;
        consumer.wakeup();
    }
}
