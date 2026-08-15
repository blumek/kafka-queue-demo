package dev.blumek.inference.gateway.health;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.KafkaFuture;
import org.jspecify.annotations.NonNull;
import org.springframework.boot.health.contributor.AbstractHealthIndicator;
import org.springframework.boot.health.contributor.Health;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

class KafkaHealthIndicator extends AbstractHealthIndicator {
    private static final Duration AWAIT_GRACE = Duration.ofMillis(500);

    private final Admin admin;
    private final Set<String> requiredTopics;
    private final Duration timeout;

    KafkaHealthIndicator(final Admin admin, final Set<String> requiredTopics, final Duration timeout) {
        this.admin = requireNonNull(admin);
        this.requiredTopics = Set.copyOf(requireNonNull(requiredTopics));
        this.timeout = requireNonNull(timeout);
    }

    @Override
    protected void doHealthCheck(final Health.Builder builder) throws Exception {
        final var timeoutMs = Math.toIntExact(timeout.toMillis());
        final var cluster = admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMs));
        final var topics = admin.listTopics(new ListTopicsOptions().timeoutMs(timeoutMs));

        final var clusterId = await(cluster.clusterId());
        final var nodes = await(cluster.nodes());
        final var present = await(topics.names());
        final var missing = missingTopics(present);

        builder.withDetail("clusterId", clusterId).withDetail("nodes", nodes.size());
        if (missing.isEmpty()) {
            builder.up();
        } else {
            builder.down().withDetail("missingTopics", missing);
        }
    }

    private <T> T await(final KafkaFuture<T> future) throws Exception {
        try {
            return future.get(timeout.plus(AWAIT_GRACE).toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (final ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
    }

    private List<String> missingTopics(final Set<String> present) {
        return requiredTopics.stream()
                .filter(topic -> !present.contains(topic))
                .sorted()
                .toList();
    }
}
