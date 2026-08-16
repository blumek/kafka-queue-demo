package dev.blumek.inference.worker.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.messaging.InferenceSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Map;

public final class ShareConsumerFactory {
    public static final String GROUP = "inference-workers";
    private static final String EXPLICIT = "explicit";
    private static final String RECORD_LIMIT = "record_limit";

    private ShareConsumerFactory() {}

    public static Map<String, Object> consumerConfiguration(final String bootstrapServers, final int maxPollRecords) {
        return consumerConfiguration(bootstrapServers, GROUP, maxPollRecords);
    }

    public static Map<String, Object> consumerConfiguration(final String bootstrapServers,
                                                            final String group,
                                                            final int maxPollRecords) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, group,
                ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, EXPLICIT,
                ConsumerConfig.SHARE_ACQUIRE_MODE_CONFIG, RECORD_LIMIT,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
    }

    public static ShareConsumer<String, InferenceRequest> jobConsumer(final Map<String, Object> configuration) {
        return new KafkaShareConsumer<>(configuration, new StringDeserializer(), InferenceSerdes.requestDeserializer());
    }
}
