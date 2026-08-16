package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.messaging.InferenceTopics;
import dev.blumek.inference.messaging.JobHeaders;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

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
    private static final String PRINT_KEY = "print.key=true";
    private static final String PRINT_HEADERS = "print.headers=true";

    private static final String A_RETRIED_KEY = "8f14e45f-ceea-467a-9c1e-1b5c5a2d3e4f";
    private static final String A_STATUS_URL_KEY = "c9bf9e57-1685-4c89-bafb-ff5af830be8a";
    private static final String ONE_KEY = "6ea2f0d8-6b4a-4f0e-9a3d-7c8b9e0a1b2c";
    private static final String ANOTHER_KEY = "2d1a3b4c-5e6f-4071-8293-a4b5c6d7e8f9";
    private static final String A_TOPIC_KEY = "f47ac10b-58cc-4372-a567-0e02b2c3d479";

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

    @Test
    void stampsTheRecordWithTheTraceTheSubmissionStarted() throws Exception {
        final var response = whenAJobIsSubmitted();

        assertThat(whenTheConsoleConsumerReadsTheHeadersOfTheRecordKeyedBy(response.jobId()))
                .contains(JobHeaders.TRACEPARENT + ":00-");
    }

    @Test
    void answersARetryCarryingTheSameKeyWithTheJobIdItAlreadyIssued() {
        final var actualFirst = whenAJobIsSubmittedWithKey(A_RETRIED_KEY);
        final var actualRetry = whenAJobIsSubmittedWithKey(A_RETRIED_KEY);

        assertThat(actualRetry.jobId()).isEqualTo(actualFirst.jobId()).isEqualTo(A_RETRIED_KEY);
    }

    @Test
    void pointsARetryAtTheStatusUrlOfTheJobItAlreadyStarted() {
        final var actualFirst = whenAJobIsSubmittedWithKey(A_STATUS_URL_KEY);
        final var actualRetry = whenAJobIsSubmittedWithKey(A_STATUS_URL_KEY);

        assertThat(actualRetry.statusUrl()).isEqualTo(actualFirst.statusUrl());
    }

    @Test
    void issuesADistinctJobForEachDistinctKey() {
        final var actualFirst = whenAJobIsSubmittedWithKey(ONE_KEY);
        final var actualSecond = whenAJobIsSubmittedWithKey(ANOTHER_KEY);

        assertThat(actualFirst.jobId()).isNotEqualTo(actualSecond.jobId());
    }

    @Test
    void keepsARetryOnTheOneJobIdRatherThanOpeningASecondJobOnTheTopic() throws Exception {
        final var actualFirst = whenAJobIsSubmittedWithKey(A_TOPIC_KEY);
        final var actualRetry = whenAJobIsSubmittedWithKey(A_TOPIC_KEY);

        assertThat(whenTheConsoleConsumerReadsRecordsKeyedBy(actualFirst.jobId()))
                .hasSize(2)
                .allSatisfy(value -> assertThat(value).contains("\"jobId\":\"" + actualRetry.jobId() + "\""));
    }

    private JobAcceptedResponse whenAJobIsSubmitted() {
        return whenSubmitting(client.post().uri("/jobs"));
    }

    private JobAcceptedResponse whenAJobIsSubmittedWithKey(final String idempotencyKey) {
        return whenSubmitting(client.post().uri("/jobs").header("Idempotency-Key", idempotencyKey));
    }

    private JobAcceptedResponse whenSubmitting(final RestTestClient.RequestBodySpec request) {
        return request.contentType(MediaType.APPLICATION_JSON)
                .body(new SubmitJobRequest("llama3:8b", "why is the sky blue?", 128))
                .exchange()
                .expectStatus().isAccepted()
                .expectHeader().exists(HttpHeaders.LOCATION)
                .expectBody(JobAcceptedResponse.class)
                .returnResult()
                .getResponseBody();
    }

    private String whenTheConsoleConsumerReadsTheRecordKeyedBy(final String jobId) throws Exception {
        final var values = whenTheConsoleConsumerReadsRecordsKeyedBy(jobId);
        assertThat(values).hasSize(1);
        return values.getFirst();
    }

    private List<String> whenTheConsoleConsumerReadsRecordsKeyedBy(final String jobId) throws Exception {
        final var drained = whenTheConsoleConsumerDrainsTheTopic(PRINT_KEY);
        return drained.lines()
                .map(line -> line.split(KEY_SEPARATOR, 2))
                .filter(parts -> parts.length == 2 && parts[0].equals(jobId))
                .map(parts -> parts[1])
                .toList();
    }

    private String whenTheConsoleConsumerReadsTheHeadersOfTheRecordKeyedBy(final String jobId) throws Exception {
        final var drained = whenTheConsoleConsumerDrainsTheTopic(PRINT_HEADERS, PRINT_KEY);
        final var headers = drained.lines()
                .map(line -> line.split(KEY_SEPARATOR, 3))
                .filter(parts -> parts.length == 3 && parts[1].equals(jobId))
                .map(parts -> parts[0])
                .toList();
        assertThat(headers).hasSize(1);
        return headers.getFirst();
    }

    private String whenTheConsoleConsumerDrainsTheTopic(final String... properties) throws Exception {
        final var command = new ArrayList<>(List.of(CONSOLE_CONSUMER,
                "--bootstrap-server", INTERNAL_BOOTSTRAP,
                "--topic", InferenceTopics.JOBS,
                "--from-beginning",
                "--timeout-ms", IDLE_TIMEOUT_MS));
        Stream.of(properties).forEach(property -> command.addAll(List.of("--property", property)));

        final var result = kafka.execInContainer(command.toArray(String[]::new));
        assertThat(result.getStdout()).as("console consumer stderr: %s", result.getStderr()).isNotBlank();
        return result.getStdout();
    }
}
