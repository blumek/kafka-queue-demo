package dev.blumek.inference.worker.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ShareConsumerFactoryTest {
    private static final String BROKER = "localhost:9092";
    private static final int CONCURRENCY = 4;

    private final Map<String, Object> configuration =
            ShareConsumerFactory.consumerConfiguration(BROKER, CONCURRENCY);

    @Test
    void acknowledgesEveryRecordExplicitly() {
        assertThat(configuration).containsEntry(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
    }

    @Test
    void letsTheBrokerCapAFetchAtTheInstanceConcurrency() {
        assertThat(configuration)
                .containsEntry(ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, "record_limit")
                .containsEntry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, CONCURRENCY);
    }

    @Test
    void joinsTheOneShareGroupEveryWorkerBelongsTo() {
        assertThat(configuration).containsEntry(ConsumerConfig.GROUP_ID_CONFIG, "inference-workers");
    }

    @Test
    void staysWithinTheSettingsTheShareConsumerAccepts() {
        assertThat(configuration).doesNotContainKeys(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConsumerConfig.GROUP_PROTOCOL_CONFIG,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG);
    }
}
