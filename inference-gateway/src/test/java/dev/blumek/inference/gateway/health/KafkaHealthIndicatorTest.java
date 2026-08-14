package dev.blumek.inference.gateway.health;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.health.contributor.Status;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaHealthIndicatorTest {
    private static final String CLUSTER_ID = "5L6g3nShT-eMCtK--X86sw";
    private static final String REQUIRED_TOPIC = "inference.jobs";
    private static final String OTHER_TOPIC = "inference.results";
    private static final Duration TIMEOUT = Duration.ofSeconds(2);
    private static final int TIMEOUT_MS = 2_000;

    private final Admin admin = mock(Admin.class);
    private final DescribeClusterResult cluster = mock(DescribeClusterResult.class);
    private final ListTopicsResult topics = mock(ListTopicsResult.class);

    private final KafkaHealthIndicator indicator = new KafkaHealthIndicator(admin, Set.of(REQUIRED_TOPIC), TIMEOUT);

    @Test
    void reportsUpWhenTheClusterAnswersAndTheRequiredTopicExists() {
        givenTheClusterAnswers();
        givenTheBrokerHasTopics(REQUIRED_TOPIC, OTHER_TOPIC);

        final var actualHealth = indicator.health();

        assertThat(actualHealth.getStatus()).isEqualTo(Status.UP);
    }

    private void givenTheClusterAnswers() {
        when(admin.describeCluster(any())).thenReturn(cluster);
        when(cluster.clusterId()).thenReturn(KafkaFuture.completedFuture(CLUSTER_ID));
        when(cluster.nodes()).thenReturn(KafkaFuture.<Collection<Node>>completedFuture(List.of(new Node(1, "localhost", 9092))));
    }

    private void givenTheBrokerHasTopics(final String... names) {
        when(admin.listTopics(any())).thenReturn(topics);
        when(topics.names()).thenReturn(KafkaFuture.completedFuture(Set.of(names)));
    }

    @Test
    void reportsTheClusterIdAndNodeCountAsDetails() {
        givenTheClusterAnswers();
        givenTheBrokerHasTopics(REQUIRED_TOPIC);

        final var actualHealth = indicator.health();

        assertThat(actualHealth.getDetails()).containsEntry("clusterId", CLUSTER_ID).containsEntry("nodes", 1);
    }

    @Test
    void reportsDownAndNamesTheMissingTopics() {
        givenTheClusterAnswers();
        givenTheBrokerHasTopics(OTHER_TOPIC);

        final var actualHealth = indicator.health();

        assertThat(actualHealth.getStatus()).isEqualTo(Status.DOWN);
        assertThat(actualHealth.getDetails()).containsEntry("missingTopics", List.of(REQUIRED_TOPIC));
    }

    @Test
    void reportsDownWhenTheClusterCallTimesOut() {
        givenTheClusterTimesOut();
        givenTheBrokerHasTopics(REQUIRED_TOPIC);

        final var actualHealth = indicator.health();

        assertThat(actualHealth.getStatus()).isEqualTo(Status.DOWN);
    }

    private void givenTheClusterTimesOut() {
        final var failed = new KafkaFutureImpl<String>();
        failed.completeExceptionally(new TimeoutException("Timed out waiting for a node assignment. Call: listNodes"));
        when(admin.describeCluster(any())).thenReturn(cluster);
        when(cluster.clusterId()).thenReturn(failed);
    }

    @Test
    void unwrapsTheKafkaTimeoutFromTheExecutionException() {
        givenTheClusterTimesOut();
        givenTheBrokerHasTopics(REQUIRED_TOPIC);

        final var actualHealth = indicator.health();

        assertThat(actualHealth.getDetails())
                .containsEntry("error", TimeoutException.class.getName() + ": Timed out waiting for a node assignment. Call: listNodes");
    }

    @Test
    void boundsEveryAdminCallWithTheConfiguredTimeout() {
        givenTheClusterAnswers();
        givenTheBrokerHasTopics(REQUIRED_TOPIC);

        indicator.health();

        final var actualClusterOptions = ArgumentCaptor.forClass(DescribeClusterOptions.class);
        final var actualTopicOptions = ArgumentCaptor.forClass(ListTopicsOptions.class);
        verify(admin).describeCluster(actualClusterOptions.capture());
        verify(admin).listTopics(actualTopicOptions.capture());
        assertThat(actualClusterOptions.getValue().timeoutMs()).isEqualTo(TIMEOUT_MS);
        assertThat(actualTopicOptions.getValue().timeoutMs()).isEqualTo(TIMEOUT_MS);
    }

    @Test
    void restoresTheInterruptFlagWhenTheCheckIsInterrupted() {
        givenTheClusterNeverAnswers();
        givenTheBrokerHasTopics(REQUIRED_TOPIC);
        Thread.currentThread().interrupt();

        final var actualHealth = indicator.health();

        assertThat(Thread.interrupted()).isTrue();
        assertThat(actualHealth.getStatus()).isEqualTo(Status.DOWN);
    }

    private void givenTheClusterNeverAnswers() {
        when(admin.describeCluster(any())).thenReturn(cluster);
        when(cluster.clusterId()).thenReturn(new KafkaFutureImpl<>());
    }

    @Test
    void rejectsANullAdmin() {
        assertThatNullPointerException()
                .isThrownBy(() -> new KafkaHealthIndicator(null, Set.of(REQUIRED_TOPIC), TIMEOUT));
    }

    @Test
    void rejectsNullRequiredTopics() {
        assertThatNullPointerException().isThrownBy(() -> new KafkaHealthIndicator(admin, null, TIMEOUT));
    }

    @Test
    void rejectsANullTimeout() {
        assertThatNullPointerException()
                .isThrownBy(() -> new KafkaHealthIndicator(admin, Set.of(REQUIRED_TOPIC), null));
    }
}
