package dev.blumek.inference.worker.processing;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.InferenceResult;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.domain.port.EngineOutcome;
import dev.blumek.inference.domain.port.InferenceEngine;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JobProcessorTest {
    private static final Instant SUBMITTED_AT = Instant.parse("2026-08-16T09:00:00Z");
    private static final int SECOND_DELIVERY = 2;
    private static final Disposition ACCEPTED = new Disposition.Accept();

    private final List<InferenceRequest> engineCalls = new ArrayList<>();
    private final List<EngineOutcome> classifiedOutcomes = new ArrayList<>();
    private final List<Integer> classifiedDeliveryCounts = new ArrayList<>();

    @Test
    void handsTheRequestToTheEngine() {
        final var request = givenARequest();

        givenAProcessor(givenTheEngineCompletes()).process(request, SECOND_DELIVERY);

        assertThat(engineCalls).containsExactly(request);
    }

    @Test
    void classifiesWhateverTheEngineAnswered() {
        final var failure = new EngineOutcome.Failed.Unavailable("the engine refused the connection");

        givenAProcessor(_ -> failure).process(givenARequest(), SECOND_DELIVERY);

        assertThat(classifiedOutcomes).containsExactly(failure);
    }

    @Test
    void tellsTheClassifierHowOftenTheJobHasBeenDelivered() {
        givenAProcessor(givenTheEngineCompletes()).process(givenARequest(), SECOND_DELIVERY);

        assertThat(classifiedDeliveryCounts).containsExactly(SECOND_DELIVERY);
    }

    @Test
    void answersWithTheDispositionTheClassifierChose() {
        final var actualDisposition = givenAProcessor(givenTheEngineCompletes()).process(givenARequest(), SECOND_DELIVERY);

        assertThat(actualDisposition).isEqualTo(ACCEPTED);
    }

    @Test
    void leavesAnEngineDefectToTheCallerToContain() {
        final InferenceEngine broken = _ -> {
            throw new IllegalStateException("the engine client was not configured");
        };

        assertThatThrownBy(() -> givenAProcessor(broken).process(givenARequest(), SECOND_DELIVERY))
                .isInstanceOf(IllegalStateException.class);
    }

    private JobProcessor givenAProcessor(final InferenceEngine engine) {
        return new JobProcessor(request -> {
            engineCalls.add(request);
            return engine.run(request);
        }, (outcome, deliveryCount) -> {
            classifiedOutcomes.add(outcome);
            classifiedDeliveryCounts.add(deliveryCount);
            return ACCEPTED;
        });
    }

    private static InferenceEngine givenTheEngineCompletes() {
        return request -> new EngineOutcome.Completed(new InferenceResult(request.jobId(), request.model(),
                "because of Rayleigh scattering", 5, 6, Duration.ofMillis(120), SUBMITTED_AT));
    }

    private static InferenceRequest givenARequest() {
        return new InferenceRequest(JobId.newId(), new ModelId("llama3:8b"), "why is the sky blue?", 128, SUBMITTED_AT);
    }
}
