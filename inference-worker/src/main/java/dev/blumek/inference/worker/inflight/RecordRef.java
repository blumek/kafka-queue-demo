package dev.blumek.inference.worker.inflight;

import org.apache.kafka.common.TopicPartition;

import static java.util.Objects.requireNonNull;

public record RecordRef(TopicPartition partition, long offset) {

    public RecordRef {
        requireNonNull(partition);
    }
}
