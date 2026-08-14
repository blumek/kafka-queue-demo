package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class CompletionSynthesiserTest {
    private static final String MODEL = "llama3:8b";
    private static final int VOCABULARY_SIZE = 16;

    private final CompletionSynthesiser synthesiser = new CompletionSynthesiser();

    @Test
    void labelsTheCompletionWithTheRequestedModel() {
        final var givenRequest = givenRequest();

        final var actualCompletion = synthesiser.synthesise(givenRequest, 3);

        assertThat(actualCompletion).startsWith("[sim " + MODEL + "] ");
    }

    private InferenceRequest givenRequest() {
        return new InferenceRequest(new JobId("job-a"), new ModelId(MODEL), "why is the sky blue?", 64, Instant.EPOCH);
    }

    @Test
    void emitsOneWordPerCompletionToken() {
        final var givenRequest = givenRequest();

        final var actualCompletion = synthesiser.synthesise(givenRequest, 5);

        assertThat(wordsOf(actualCompletion)).hasSize(5);
    }

    private String[] wordsOf(final String completion) {
        return completion.substring(completion.indexOf(']') + 2).split(" ");
    }

    @Test
    void terminatesTheLastWordWithAFullStop() {
        final var givenRequest = givenRequest();

        final var actualCompletion = synthesiser.synthesise(givenRequest, 4);

        assertThat(actualCompletion).endsWith(".");
    }

    @Test
    void wrapsAroundTheVocabularyBeyondItsLength() {
        final var givenRequest = givenRequest();

        final var actualCompletion = synthesiser.synthesise(givenRequest, VOCABULARY_SIZE + 1);

        assertThat(wordsOf(actualCompletion)).endsWith("the.");
    }

    @Test
    void producesOnlyTheHeaderForZeroTokens() {
        final var givenRequest = givenRequest();

        final var actualCompletion = synthesiser.synthesise(givenRequest, 0);

        assertThat(actualCompletion).isEqualTo("[sim " + MODEL + "] ");
    }

    @Test
    void isReproducibleForTheSameInput() {
        final var givenRequest = givenRequest();

        final var actualCompletion = synthesiser.synthesise(givenRequest, 9);

        assertThat(actualCompletion).isEqualTo(synthesiser.synthesise(givenRequest, 9));
    }
}
