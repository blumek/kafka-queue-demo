package dev.blumek.inference.worker.inflight;

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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class InFlightRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(InFlightRegistry.class);

    private final Map<RecordRef, InFlight> entries = new HashMap<>();
    private final Queue<Completion> completions = new ConcurrentLinkedQueue<>();
    private final Clock clock;
    private final int maxInFlight;
    private final int maxRenewals;

    public InFlightRegistry(final Clock clock, final int maxInFlight, final int maxRenewals) {
        this.clock = clock;
        this.maxInFlight = maxInFlight;
        this.maxRenewals = maxRenewals;
    }

    public void track(final InFlight inFlight) {
        rejectWhenFull(inFlight.ref());
        entries.put(inFlight.ref(), inFlight);
        warnWhenRenewalsAreExhausted(inFlight);
    }

    private void rejectWhenFull(final RecordRef ref) {
        if (entries.size() >= maxInFlight && !entries.containsKey(ref)) {
            throw new IllegalStateException("Tracking %s would exceed the in-flight limit of %d"
                    .formatted(ref, maxInFlight));
        }
    }

    private void warnWhenRenewalsAreExhausted(final InFlight inFlight) {
        if (inFlight.renewals() == maxRenewals) {
            LOG.warn("Lock for {} was renewed {} times and will not be renewed again; "
                    + "the record will lapse and be redelivered", inFlight.ref(), maxRenewals);
        }
    }

    public void complete(final Completion completion) {
        completions.add(completion);
    }

    public List<Completion> drainCompleted() {
        final var drained = new ArrayList<Completion>();
        for (var completion = completions.poll(); completion != null; completion = completions.poll()) {
            forget(completion.ref());
            drained.add(completion);
        }
        return List.copyOf(drained);
    }

    private void forget(final RecordRef ref) {
        if (entries.remove(ref) == null) {
            throw new IllegalStateException("Completion for untracked record " + ref);
        }
    }

    public List<InFlight> needingRenewal(final Duration lockTimeout) {
        final var threshold = lockTimeout.dividedBy(2);
        final var now = clock.instant();
        return entries.values().stream()
                .filter(inFlight -> inFlight.heldSinceLockRefresh(now).compareTo(threshold) >= 0)
                .filter(inFlight -> inFlight.renewals() < maxRenewals)
                .toList();
    }

    public List<InFlight> abandonAll() {
        final var abandoned = List.copyOf(entries.values());
        entries.clear();
        completions.clear();
        return abandoned;
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
