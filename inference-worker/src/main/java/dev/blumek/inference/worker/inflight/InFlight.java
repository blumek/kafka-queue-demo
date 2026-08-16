package dev.blumek.inference.worker.inflight;

import dev.blumek.inference.domain.model.InferenceRequest;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public record InFlight(RecordRef ref,
                       InferenceRequest request,
                       int deliveryCount,
                       Instant acquiredAt,
                       Instant lockRefreshedAt,
                       int renewals) {
    private static final short UNCOUNTED_DELIVERY = 1;

    public InFlight {
        requireNonNull(ref);
        requireNonNull(request);
        requireNonNull(acquiredAt);
        requireNonNull(lockRefreshedAt);
    }

    public static InFlight of(final ConsumerRecord<String, InferenceRequest> record, final Instant acquiredAt) {
        return acquired(RecordRef.of(record), record.value(), record.deliveryCount().orElse(UNCOUNTED_DELIVERY),
                acquiredAt);
    }

    public static InFlight acquired(final RecordRef ref,
                                    final InferenceRequest request,
                                    final int deliveryCount,
                                    final Instant acquiredAt) {
        return new InFlight(ref, request, deliveryCount, acquiredAt, acquiredAt, 0);
    }

    public InFlight refreshed(final Instant refreshedAt) {
        return new InFlight(ref, request, deliveryCount, acquiredAt, refreshedAt, renewals + 1);
    }

    public Duration heldSinceLockRefresh(final Instant now) {
        return Duration.between(lockRefreshedAt, now);
    }
}
