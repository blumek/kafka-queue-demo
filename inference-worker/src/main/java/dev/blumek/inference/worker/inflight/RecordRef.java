package dev.blumek.inference.worker.inflight;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import static java.util.Objects.requireNonNull;

public record RecordRef(TopicPartition partition, long offset) {

    public RecordRef {
        requireNonNull(partition);
    }

    public static RecordRef of(final ConsumerRecord<?, ?> record) {
        return new RecordRef(new TopicPartition(record.topic(), record.partition()), record.offset());
    }
}
