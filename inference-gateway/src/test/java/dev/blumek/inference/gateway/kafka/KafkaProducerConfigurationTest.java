package dev.blumek.inference.gateway.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class KafkaProducerConfigurationTest {
    private static final List<String> BOOTSTRAP_SERVERS = List.of("localhost:9092", "localhost:9093");

    @Test
    void acknowledgesOnlyAfterEveryInSyncReplicaHasTheRecord() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        assertThat(actualConfiguration).containsEntry(ProducerConfig.ACKS_CONFIG, "all");
    }

    private Map<String, Object> whenTheProducerIsConfigured() {
        return KafkaProducerConfiguration.producerConfiguration(Map.of(), BOOTSTRAP_SERVERS);
    }

    @Test
    void enablesIdempotenceSoRetriesCannotDuplicateAJob() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        assertThat(actualConfiguration).containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    }

    @Test
    void keepsInFlightRequestsWithinTheIdempotentProducerLimit() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        assertThat(actualConfiguration).containsEntry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    }

    @Test
    void boundsDeliveryAtThirtySeconds() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        assertThat(actualConfiguration).containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
    }

    @Test
    void compressesBatchesWithLz4() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        assertThat(actualConfiguration).containsEntry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    }

    @Test
    void lingersBrieflySoRecordsBatch() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        assertThat(actualConfiguration).containsEntry(ProducerConfig.LINGER_MS_CONFIG, 5);
    }

    @Test
    void keepsTheDeliveryBudgetAboveLingerPlusTheRequestTimeout() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        final var actualFloor = (int) actualConfiguration.get(ProducerConfig.LINGER_MS_CONFIG)
                + (int) actualConfiguration.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        assertThat((int) actualConfiguration.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG))
                .isGreaterThanOrEqualTo(actualFloor);
    }

    @Test
    void buildsAConfigurationTheProducerAccepts() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        assertThatNoException().isThrownBy(() ->
                new KafkaProducer<>(actualConfiguration, new StringSerializer(), new StringSerializer()).close());
    }

    @Test
    void takesTheBootstrapServersFromTheConnectionDetails() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        assertThat(actualConfiguration).containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    }

    @Test
    void overridesTheBootstrapServersCarriedByTheBaseProperties() {
        final var base = Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, List.of("stale-broker:9092"));

        final var actualConfiguration = KafkaProducerConfiguration.producerConfiguration(base, BOOTSTRAP_SERVERS);

        assertThat(actualConfiguration).containsEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    }

    @Test
    void keepsBasePropertiesItDoesNotOverride() {
        final var base = Map.<String, Object>of(ProducerConfig.BUFFER_MEMORY_CONFIG, 1_024L);

        final var actualConfiguration = KafkaProducerConfiguration.producerConfiguration(base, BOOTSTRAP_SERVERS);

        assertThat(actualConfiguration).containsEntry(ProducerConfig.BUFFER_MEMORY_CONFIG, 1_024L);
    }

    @Test
    void boundsHowLongSendMayBlockTheCallingThread() {
        final var actualConfiguration = whenTheProducerIsConfigured();

        assertThat(actualConfiguration).containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000L);
    }

    @Test
    void boundsTheQueueOfSendsWaitingForAThread() {
        try (final var actualExecutor = whenTheSendExecutorIsConfigured()) {
            assertThat(actualExecutor.getQueue().remainingCapacity()).isEqualTo(64);
        }
    }

    private ThreadPoolExecutor whenTheSendExecutorIsConfigured() {
        return (ThreadPoolExecutor) new KafkaProducerConfiguration().jobSendExecutor();
    }

    @Test
    void shedsSendsInsteadOfQueueingThemWithoutBound() {
        try (final var actualExecutor = whenTheSendExecutorIsConfigured()) {
            assertThat(actualExecutor.getRejectedExecutionHandler())
                    .isInstanceOf(ThreadPoolExecutor.AbortPolicy.class);
        }
    }

    @Test
    void namesTheSendThreadsSoAStackDumpShowsWhereARequestIsStuck() throws Exception {
        try (final var actualExecutor = whenTheSendExecutorIsConfigured()) {
            assertThat(actualExecutor.submit(() -> Thread.currentThread().getName()).get())
                    .startsWith("job-send-");
        }
    }
}
