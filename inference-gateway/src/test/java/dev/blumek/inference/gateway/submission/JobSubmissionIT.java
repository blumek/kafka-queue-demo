package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.messaging.InferenceTopics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(JobSubmissionIT.Containers.class)
class JobSubmissionIT {
    private static final int PARTITIONS = 6;
    private static final short REPLICAS = 1;
    private static final String CONSOLE_CONSUMER = "/opt/kafka/bin/kafka-console-consumer.sh";

    private static final String INTERNAL_BOOTSTRAP = "localhost:9093";
    private static final String IDLE_TIMEOUT_MS = "5000";

    private static final String KEY_SEPARATOR = "\t";

    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() {
            return new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        }
    }

    @Autowired
    private KafkaContainer kafka;

    @LocalServerPort
    private int port;

    private RestTestClient client;

    @BeforeEach
    void givenTheJobsTopicExists() throws Exception {
        client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        try (final var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(InferenceTopics.JOBS, PARTITIONS, REPLICAS))).all().get();
        } catch (final ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    @Test
    void acceptsASubmittedJobAndPointsTheCallerAtItsStatusUrl() {
        final var actualResponse = whenAJobIsSubmitted();

        assertThat(actualResponse.statusUrl().getPath()).isEqualTo("/jobs/" + actualResponse.jobId());
    }

    @Test
    void issuesADistinctJobIdForEverySubmission() {
        final var actualFirst = whenAJobIsSubmitted();
        final var actualSecond = whenAJobIsSubmitted();

        assertThat(actualFirst.jobId()).isNotEqualTo(actualSecond.jobId());
    }

    @Test
    void showsTheSubmittedJobOnTheJobsTopicToAConsoleConsumer() throws Exception {
        final var response = whenAJobIsSubmitted();

        final var actualRecord = whenTheConsoleConsumerReadsTheRecordKeyedBy(response.jobId());

        assertThat(actualRecord)
                .contains("\"jobId\":\"" + response.jobId() + "\"")
                .contains("\"model\":\"llama3:8b\"")
                .contains("\"prompt\":\"why is the sky blue?\"")
                .contains("\"maxTokens\":128");
    }

    @Test
    void keysTheRecordByTheJobIdItIssued() throws Exception {
        final var response = whenAJobIsSubmitted();

        assertThat(whenTheConsoleConsumerReadsTheRecordKeyedBy(response.jobId())).isNotBlank();
    }

    private JobAcceptedResponse whenAJobIsSubmitted() {
        return client.post()
                .uri("/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .body(new SubmitJobRequest("llama3:8b", "why is the sky blue?", 128))
                .exchange()
                .expectStatus().isAccepted()
                .expectHeader().exists(HttpHeaders.LOCATION)
                .expectBody(JobAcceptedResponse.class)
                .returnResult()
                .getResponseBody();
    }

    private String whenTheConsoleConsumerReadsTheRecordKeyedBy(final String jobId) throws Exception {
        final var drained = whenTheConsoleConsumerDrainsTheTopic();
        final var values = drained.lines()
                .map(line -> line.split(KEY_SEPARATOR, 2))
                .filter(parts -> parts.length == 2 && parts[0].equals(jobId))
                .map(parts -> parts[1])
                .toList();
        assertThat(values).as("console consumer output: %s", drained).hasSize(1);
        return values.getFirst();
    }

    private String whenTheConsoleConsumerDrainsTheTopic() throws Exception {
        final var result = kafka.execInContainer(CONSOLE_CONSUMER,
                "--bootstrap-server", INTERNAL_BOOTSTRAP,
                "--topic", InferenceTopics.JOBS,
                "--from-beginning",
                "--timeout-ms", IDLE_TIMEOUT_MS,
                "--property", "print.key=true");
        assertThat(result.getStdout()).as("console consumer stderr: %s", result.getStderr()).isNotBlank();
        return result.getStdout();
    }
}
