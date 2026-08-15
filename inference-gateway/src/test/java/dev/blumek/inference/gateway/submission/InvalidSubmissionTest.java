package dev.blumek.inference.gateway.submission;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
                properties = "spring.kafka.bootstrap-servers=localhost:1")
class InvalidSubmissionTest {
    private static final String MALFORMED_MODEL = """
            {"model": "Llama3:8B", "prompt": "why is the sky blue?", "maxTokens": 128}""";
    private static final String BLANK_PROMPT = """
            {"model": "llama3:8b", "prompt": "   ", "maxTokens": 128}""";
    private static final String OVERSIZED_TOKEN_BUDGET = """
            {"model": "llama3:8b", "prompt": "why is the sky blue?", "maxTokens": 100000}""";
    private static final String NOT_JSON = "{\"model\": ";

    @LocalServerPort
    private int port;

    @Test
    void refusesAModelIdTheDomainCouldNotBuild() {
        whenSubmitting(MALFORMED_MODEL).expectStatus().isBadRequest();
    }

    @Test
    void describesTheRefusalAsAProblemDocument() {
        whenSubmitting(MALFORMED_MODEL).expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
    }

    @Test
    void namesTheOffendingFieldSoTheClientKnowsWhatToFix() {
        whenSubmitting(MALFORMED_MODEL).expectBody().jsonPath("$.errors.model").exists();
    }

    @Test
    void carriesTheStatusInsideTheProblemDocumentAsWellAsTheResponseLine() {
        whenSubmitting(MALFORMED_MODEL).expectBody().jsonPath("$.status").isEqualTo(400);
    }

    @Test
    void pointsTheProblemDocumentAtTheEndpointThatRefusedTheSubmission() {
        whenSubmitting(MALFORMED_MODEL).expectBody().jsonPath("$.instance").isEqualTo("/jobs");
    }

    @Test
    void doesNotHandOutAJobIdForASubmissionItRefused() {
        whenSubmitting(MALFORMED_MODEL).expectBody().jsonPath("$.jobId").doesNotExist();
    }

    @Test
    void doesNotPointTheClientAtAStatusUrlForASubmissionItRefused() {
        whenSubmitting(MALFORMED_MODEL).expectHeader().doesNotExist(HttpHeaders.LOCATION);
    }

    @Test
    void namesThePromptWhenItIsBlank() {
        whenSubmitting(BLANK_PROMPT).expectBody().jsonPath("$.errors.prompt").exists();
    }

    @Test
    void namesTheTokenBudgetWhenItIsBeyondWhatTheEngineSupports() {
        whenSubmitting(OVERSIZED_TOKEN_BUDGET).expectBody().jsonPath("$.errors.maxTokens").exists();
    }

    @Test
    void answersUnparseableJsonWithAProblemDocumentRatherThanAStackTrace() {
        whenSubmitting(NOT_JSON).expectStatus().isBadRequest()
                .expectHeader().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON);
    }

    private RestTestClient.ResponseSpec whenSubmitting(final String submission) {
        return RestTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build()
                .post()
                .uri("/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                .body(submission)
                .exchange();
    }
}
