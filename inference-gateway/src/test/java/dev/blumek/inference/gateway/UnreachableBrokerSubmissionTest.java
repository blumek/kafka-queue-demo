package dev.blumek.inference.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
                properties = "spring.kafka.bootstrap-servers=localhost:1")
class UnreachableBrokerSubmissionTest {
    private static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(30);
    private static final String SUBMISSION = """
            {"model": "llama3:8b", "prompt": "why is the sky blue?", "maxTokens": 128}""";

    @LocalServerPort
    private int port;

    @Test
    void refusesTheJobRatherThanAcceptingOneItCouldNotQueue() {
        whenAJobIsSubmitted().expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void tellsTheClientHowLongToWaitBeforeResubmitting() {
        whenAJobIsSubmitted().expectHeader().valueEquals(HttpHeaders.RETRY_AFTER, "5");
    }

    @Test
    void doesNotHandTheClientAJobIdItNeverQueued() {
        whenAJobIsSubmitted().expectBody().jsonPath("$.jobId").doesNotExist();
    }

    @Test
    void explainsTheRefusalAsAProblemDocument() {
        whenAJobIsSubmitted().expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON)
                .expectBody().jsonPath("$.title").isEqualTo("Job queue unavailable");
    }

    @Test
    void answersWithinTheDeliveryBudgetInsteadOfHanging() {
        final var startedAt = System.nanoTime();

        whenAJobIsSubmitted().expectStatus().isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);

        assertThat(Duration.ofNanos(System.nanoTime() - startedAt)).isLessThan(DELIVERY_TIMEOUT);
    }

    private RestTestClient.ResponseSpec whenAJobIsSubmitted() {
        return RestTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build()
                .post()
                .uri("/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .body(SUBMISSION)
                .exchange();
    }
}
