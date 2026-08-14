package dev.blumek.inference.gateway.health;

import dev.blumek.inference.messaging.InferenceTopics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;
import java.util.HashMap;
import java.util.Set;

@Configuration
class KafkaHealthConfiguration {
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private static final long SOCKET_SETUP_TIMEOUT_MS = 1_000L;
    private static final String CLIENT_ID = "inference-gateway-health";
    private static final Set<String> REQUIRED_TOPICS = Set.of(InferenceTopics.JOBS);

    @Bean(destroyMethod = "close")
    Admin healthAdmin(final KafkaAdmin kafkaAdmin) {
        final var configuration = new HashMap<>(kafkaAdmin.getConfigurationProperties());
        configuration.put(AdminClientConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        configuration.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Math.toIntExact(TIMEOUT.toMillis()));
        configuration.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(TIMEOUT.toMillis()));
        configuration.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, SOCKET_SETUP_TIMEOUT_MS);
        configuration.put(AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, SOCKET_SETUP_TIMEOUT_MS);
        configuration.put(AdminClientConfig.RETRIES_CONFIG, 0);
        return Admin.create(configuration);
    }

    @Bean
    KafkaHealthIndicator kafkaHealthIndicator(final Admin healthAdmin) {
        return new KafkaHealthIndicator(healthAdmin, REQUIRED_TOPICS, TIMEOUT);
    }
}
