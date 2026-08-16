package dev.blumek.inference.worker.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.worker.inflight.RecordRef;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.AcknowledgementCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.KafkaMetric;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

final class FakeShareConsumer implements ShareConsumer<String, InferenceRequest> {

    record Acknowledgement(RecordRef ref, AcknowledgeType type) {}

    private final int lockTimeoutMs;
    private final Deque<List<ConsumerRecord<String, InferenceRequest>>> awaitingAcquisition = new ArrayDeque<>();
    private final Map<RecordRef, ConsumerRecord<String, InferenceRequest>> acquired = new LinkedHashMap<>();
    private final Map<RecordRef, ConsumerRecord<String, InferenceRequest>> renewing = new LinkedHashMap<>();
    private final Set<RecordRef> acknowledged = new HashSet<>();
    private final List<Acknowledgement> acknowledgements = new CopyOnWriteArrayList<>();
    private final Set<RecordRef> lockLost = ConcurrentHashMap.newKeySet();

    private Set<String> subscription = Set.of();
    private volatile boolean wokenUp;

    FakeShareConsumer(final Duration lockTimeout) {
        this.lockTimeoutMs = Math.toIntExact(lockTimeout.toMillis());
    }

    synchronized void deliver(final List<ConsumerRecord<String, InferenceRequest>> batch) {
        awaitingAcquisition.add(batch);
        notifyAll();
    }

    void loseTheLockOf(final RecordRef ref) {
        lockLost.add(ref);
    }

    List<Acknowledgement> acknowledgements() {
        return List.copyOf(acknowledgements);
    }

    List<AcknowledgeType> acknowledgementsOf(final RecordRef ref) {
        return acknowledgements.stream()
                .filter(acknowledgement -> acknowledgement.ref().equals(ref))
                .map(Acknowledgement::type)
                .toList();
    }

    @Override
    public synchronized ConsumerRecords<String, InferenceRequest> poll(final Duration timeout) {
        throwWhenWokenUp();
        rejectUnacknowledgedRecords();
        acquired.keySet().removeAll(acknowledged);
        acknowledged.clear();
        acquired.putAll(renewing);
        renewing.clear();
        if (acquired.isEmpty()) {
            if (awaitingAcquisition.isEmpty()) {
                awaitDelivery(timeout);
                throwWhenWokenUp();
            }
            Optional.ofNullable(awaitingAcquisition.poll())
                    .ifPresent(batch -> batch.forEach(record -> acquired.put(RecordRef.of(record), record)));
        }
        return new ConsumerRecords<>(byPartition(), Map.of());
    }

    private void throwWhenWokenUp() {
        if (wokenUp) {
            wokenUp = false;
            throw new WakeupException();
        }
    }

    private void rejectUnacknowledgedRecords() {
        if (acquired.size() != acknowledged.size()) {
            throw new IllegalStateException("All records must be acknowledged in explicit acknowledgement mode.");
        }
    }

    private void awaitDelivery(final Duration timeout) {
        try {
            wait(Math.max(1L, timeout.toMillis()));
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private Map<TopicPartition, List<ConsumerRecord<String, InferenceRequest>>> byPartition() {
        return acquired.values().stream()
                .collect(Collectors.groupingBy(record -> new TopicPartition(record.topic(), record.partition()),
                        Collectors.toCollection(ArrayList::new)));
    }

    @Override
    public synchronized void acknowledge(final ConsumerRecord<String, InferenceRequest> record,
                                         final AcknowledgeType type) {
        final var ref = RecordRef.of(record);
        if (lockLost.contains(ref)) {
            acquired.remove(ref);
            throw new IllegalStateException("The record cannot be acknowledged.");
        }
        if (!acquired.containsKey(ref) || acknowledged.contains(ref)) {
            throw new IllegalStateException("The record cannot be acknowledged.");
        }
        acknowledged.add(ref);
        acknowledgements.add(new Acknowledgement(ref, type));
        if (type == AcknowledgeType.RENEW) {
            renewing.put(ref, record);
        }
    }

    @Override
    public void acknowledge(final ConsumerRecord<String, InferenceRequest> record) {
        acknowledge(record, AcknowledgeType.ACCEPT);
    }

    @Override
    public synchronized Set<String> subscription() {
        return subscription;
    }

    @Override
    public synchronized void subscribe(final Collection<String> topics) {
        subscription = Set.copyOf(topics);
    }

    @Override
    public synchronized void unsubscribe() {
        subscription = Set.of();
    }

    @Override
    public Optional<Integer> acquisitionLockTimeoutMs() {
        return Optional.of(lockTimeoutMs);
    }

    @Override
    public synchronized void wakeup() {
        wokenUp = true;
        notifyAll();
    }

    @Override
    public void close() {
    }

    @Override
    public void close(final Duration timeout) {
    }

    @Override
    public void setAcknowledgementCommitCallback(final AcknowledgementCommitCallback callback) {
    }

    @Override
    public void acknowledge(final String topic, final int partition, final long offset, final AcknowledgeType type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<TopicIdPartition, Optional<KafkaException>> commitSync(final Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void commitAsync() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Uuid clientInstanceId(final Duration timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void registerMetricForSubscription(final KafkaMetric metric) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterMetricFromSubscription(final KafkaMetric metric) {
        throw new UnsupportedOperationException();
    }
}
