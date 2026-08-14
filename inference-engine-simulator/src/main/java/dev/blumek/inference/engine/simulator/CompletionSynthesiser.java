package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.InferenceRequest;

final class CompletionSynthesiser {
    private static final String[] WORDS = {
            "the", "model", "considers", "each", "token", "in", "turn", "and", "emits",
            "a", "plausible", "continuation", "of", "the", "given", "prompt"
    };

    String synthesise(final InferenceRequest request, final int completionTokens) {
        final var completion = new StringBuilder("[sim ").append(request.model().value()).append("] ");
        for (var i = 0; i < completionTokens; i++) {
            completion.append(WORDS[i % WORDS.length]).append(i == completionTokens - 1 ? "." : " ");
        }
        return completion.toString();
    }
}
