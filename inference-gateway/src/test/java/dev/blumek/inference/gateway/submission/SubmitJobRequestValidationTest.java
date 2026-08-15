package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.ModelId;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SubmitJobRequestValidationTest {
    private static final ValidatorFactory VALIDATORS = Validation.buildDefaultValidatorFactory();
    private static final String MODEL = "llama3:8b";
    private static final String PROMPT = "why is the sky blue?";
    private static final int MAX_TOKENS = 128;
    private static final int PROMPT_LIMIT = 32_000;
    private static final int TOKEN_CEILING = 8_192;

    private final Validator validator = VALIDATORS.getValidator();

    @Test
    void acceptsAWellFormedSubmission() {
        assertThat(whenValidated(givenASubmission())).isEmpty();
    }

    @Test
    void acceptsAModelIdWithoutATag() {
        assertThat(whenValidated(givenASubmissionForModel("llama3"))).isEmpty();
    }

    @Test
    void refusesAModelIdTheDomainWouldThrowOn() {
        assertThat(whenValidated(givenASubmissionForModel("Llama3:8B"))).containsExactly("model");
    }

    @Test
    void refusesAMissingModelId() {
        assertThat(whenValidated(givenASubmissionForModel(null))).containsExactly("model");
    }

    @Test
    void refusesABlankPrompt() {
        assertThat(whenValidated(givenASubmissionWithPrompt("   "))).containsExactly("prompt");
    }

    @Test
    void refusesAMissingPromptRatherThanLettingTheDomainFail() {
        assertThat(whenValidated(givenASubmissionWithPrompt(null))).containsExactly("prompt");
    }

    @Test
    void refusesAPromptBeyondTheSupportedLength() {
        assertThat(whenValidated(givenASubmissionWithPrompt("a".repeat(PROMPT_LIMIT + 1)))).containsExactly("prompt");
    }

    @Test
    void acceptsAPromptExactlyAtTheSupportedLength() {
        assertThat(whenValidated(givenASubmissionWithPrompt("a".repeat(PROMPT_LIMIT)))).isEmpty();
    }

    @Test
    void refusesASubmissionThatAsksForNoTokens() {
        assertThat(whenValidated(givenASubmissionForTokens(0))).containsExactly("maxTokens");
    }

    @Test
    void refusesATokenBudgetBeyondTheCeiling() {
        assertThat(whenValidated(givenASubmissionForTokens(TOKEN_CEILING + 1))).containsExactly("maxTokens");
    }

    @Test
    void acceptsATokenBudgetExactlyAtTheCeiling() {
        assertThat(whenValidated(givenASubmissionForTokens(TOKEN_CEILING))).isEmpty();
    }

    @Test
    void namesEveryOffendingFieldRatherThanStoppingAtTheFirst() {
        final var actualFields = whenValidated(new SubmitJobRequest("Llama3", " ", 0));

        assertThat(actualFields).containsExactly("maxTokens", "model", "prompt");
    }

    @Test
    void explainsAMalformedModelIdWithoutMakingTheClientReadTheRegex() {
        assertThat(whenMessagesFor(givenASubmissionForModel("Llama3:8B")))
                .containsExactly("must be a lowercase name with an optional :tag, such as llama3:8b");
    }

    @Test
    void wordsItsViolationsTheSameWhateverLocaleTheServerRunsIn() {
        final var actualMessages = whenMessagesFor(new SubmitJobRequest(MODEL, " ", TOKEN_CEILING + 1));

        assertThat(actualMessages).containsExactly("must be at most 8192", "must be provided");
    }

    @Test
    void agreesWithTheDomainOnWhichModelIdsAreAcceptable() {
        final var samples = List.of("llama3", "llama3:8b", "mistral-7b.q4", "phi_3", "Llama3", "llama3:8B", "llama 3",
                "", "   ", "llama3:", ":8b");

        samples.forEach(sample -> assertThat(whenValidated(givenASubmissionForModel(sample)).isEmpty())
                .as("model id '%s'", sample)
                .isEqualTo(whenTheDomainAcceptsIt(sample)));
    }

    private List<String> whenValidated(final SubmitJobRequest submission) {
        return validator.validate(submission).stream()
                .map(violation -> violation.getPropertyPath().toString())
                .distinct()
                .sorted()
                .toList();
    }

    private List<String> whenMessagesFor(final SubmitJobRequest submission) {
        return validator.validate(submission).stream()
                .map(violation -> violation.getMessage())
                .sorted()
                .toList();
    }

    private static boolean whenTheDomainAcceptsIt(final String model) {
        try {
            new ModelId(model);
            return true;
        } catch (final RuntimeException exception) {
            return false;
        }
    }

    private static SubmitJobRequest givenASubmission() {
        return new SubmitJobRequest(MODEL, PROMPT, MAX_TOKENS);
    }

    private static SubmitJobRequest givenASubmissionForModel(final String model) {
        return new SubmitJobRequest(model, PROMPT, MAX_TOKENS);
    }

    private static SubmitJobRequest givenASubmissionWithPrompt(final String prompt) {
        return new SubmitJobRequest(MODEL, prompt, MAX_TOKENS);
    }

    private static SubmitJobRequest givenASubmissionForTokens(final int maxTokens) {
        return new SubmitJobRequest(MODEL, PROMPT, maxTokens);
    }
}
