package dev.blumek.inference.worker.inflight;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class InFlightRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(InFlightRegistry.class);

    private final Map<RecordRef, InFlight> entries = new HashMap<>();
    private final Set<RecordRef> abandoned = new HashSet<>();
    private final BlockingQueue<Completion> completions = new LinkedBlockingQueue<>();
    private final int maxInFlight;
    private final LockMeters meters;

    public InFlightRegistry(final int maxInFlight, final LockMeters meters) {
        this.maxInFlight = maxInFlight;
        this.meters = meters;
    }

    public void track(final InFlight inFlight) {
        rejectWhenFull(inFlight.ref());
        entries.put(inFlight.ref(), inFlight);
    }

    private void rejectWhenFull(final RecordRef ref) {
        if (entries.size() >= maxInFlight && !entries.containsKey(ref)) {
            throw new IllegalStateException("Tracking %s would exceed the in-flight limit of %d"
                    .formatted(ref, maxInFlight));
        }
    }

    public Optional<InFlight> tracking(final RecordRef ref) {
        return Optional.ofNullable(entries.get(ref));
    }

    public void abandon(final RecordRef ref) {
        if (entries.remove(ref) != null) {
            abandoned.add(ref);
        }
    }

    public void complete(final Completion completion) {
        completions.add(completion);
    }

    public List<Completion> drainCompleted() {
        return awaitCompleted(Duration.ZERO);
    }

    public List<Completion> awaitCompleted(final Duration timeout) {
        final var drained = new ArrayList<Completion>();
        final var first = awaitFirstCompletion(timeout);
        first.ifPresent(drained::add);
        completions.drainTo(drained);
        return forgetAll(drained);
    }

    private Optional<Completion> awaitFirstCompletion(final Duration timeout) {
        try {
            return Optional.ofNullable(completions.poll(timeout.toMillis(), TimeUnit.MILLISECONDS));
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return Optional.empty();
        }
    }

    private List<Completion> forgetAll(final List<Completion> drained) {
        return drained.stream()
                .filter(completion -> forget(completion.ref()))
                .toList();
    }

    private boolean forget(final RecordRef ref) {
        if (abandoned.remove(ref)) {
            LOG.debug("Discarding the completion of {} because its lock was already given up", ref);
            meters.completedAfterTheLockWasGivenUp();
            return false;
        }
        final var finished = entries.remove(ref);
        if (finished == null) {
            throw new IllegalStateException("Completion for untracked record " + ref);
        }
        meters.jobFinished(finished.renewals());
        return true;
    }

    public List<InFlight> abandonAll() {
        final var abandonedEntries = List.copyOf(entries.values());
        entries.clear();
        abandoned.clear();
        completions.clear();
        return abandonedEntries;
    }

    public int size() {
        return entries.size();
    }

    public Optional<Instant> oldestAcquiredAt() {
        return entries.values().stream()
                .map(InFlight::acquiredAt)
                .min(Comparator.naturalOrder());
    }
}
