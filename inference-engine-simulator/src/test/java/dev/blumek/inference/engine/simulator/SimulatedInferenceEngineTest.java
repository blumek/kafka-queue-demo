package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.InferenceResult;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.domain.port.EngineOutcome.Completed;
import dev.blumek.inference.domain.port.EngineOutcome.Failed;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SimulatedInferenceEngineTest {
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-08-13T10:15:30Z"), ZoneOffset.UTC);
    private static final String COMPLETION = "a plausible continuation.";
    private static final long SEED = 99L;
    private static final int MAX_TOKENS = 300;
    private static final int COMPLETION_TOKENS = 42;
    private static final int ESTIMATED_PROMPT_TOKENS = 5;
    private static final int ATTEMPT = 1;
    private static final int RETRY_ATTEMPT = 3;
    private static final Duration ENGINE_LATENCY = Duration.ofMillis(7);

    private final FaultPolicy faults = mock(FaultPolicy.class);
    private final LatencyModel latency = mock(LatencyModel.class);
    private final DelaySimulator delays = mock(DelaySimulator.class);
    private final CompletionSynthesiser synthesiser = mock(CompletionSynthesiser.class);
    private final SeedCalculator seeds = mock(SeedCalculator.class);
    private final PromptTokenEstimator promptTokens = mock(PromptTokenEstimator.class);
    private final AttemptTracker attempts = mock(AttemptTracker.class);

    private final SimulatedInferenceEngine engine =
            new SimulatedInferenceEngine(faults, latency, delays, synthesiser, seeds, promptTokens, attempts, CLOCK);

    @Test
    void returnsTheFaultWhenThePolicyRejectsTheRequest() {
        final var givenRequest = givenRequest();
        givenSeedCalculatorCanCalculate();
        givenAttemptTrackerCanCount();
        givenFaultPolicyCanFailWithUnavailable();

        final var actualOutcome = engine.run(givenRequest);

        assertThat(actualOutcome).isEqualTo(givenUnavailable());
    }

    private InferenceRequest givenRequest() {
        return new InferenceRequest(new JobId("job-a"),
                                    new ModelId("llama3:8b"),
                                    "why is the sky blue?",
                                    MAX_TOKENS,
                                    Instant.EPOCH);
    }

    private void givenSeedCalculatorCanCalculate() {
        when(seeds.calculate(any())).thenReturn(SEED);
    }

    private void givenAttemptTrackerCanCount() {
        when(attempts.next(any())).thenReturn(ATTEMPT);
    }

    private void givenFaultPolicyCanFailWithUnavailable() {
        when(faults.evaluate(anyLong(), anyInt(), any())).thenReturn(Optional.of(givenUnavailable()));
    }

    private Failed givenUnavailable() {
        return new Failed.Unavailable("model rejects job");
    }

    @Test
    void leavesTheLatencyModelUntouchedWhenThePolicyRejectsTheRequest() {
        final var givenRequest = givenRequest();
        givenSeedCalculatorCanCalculate();
        givenAttemptTrackerCanCount();
        givenFaultPolicyCanFailWithTimeout();

        engine.run(givenRequest);

        verify(latency, never()).drawCompletionTokens(anyLong(), anyInt());
        verify(latency, never()).draw(anyLong(), anyInt());
    }

    private void givenFaultPolicyCanFailWithTimeout() {
        when(faults.evaluate(anyLong(), anyInt(), any())).thenReturn(Optional.of(givenTimeout()));
    }

    private Failed givenTimeout() {
        return new Failed.Timeout(Duration.ofSeconds(30));
    }

    @Test
    void assemblesTheResultFromItsCollaborators() {
        final var givenRequest = givenRequest();
        givenAllCollaboratorsCanComplete();

        final var actualOutcome = engine.run(givenRequest);

        assertThat(actualOutcome).isEqualTo(givenCompleted(givenRequest));
    }

    private Completed givenCompleted(InferenceRequest request) {
        return new Completed(new InferenceResult(request.jobId(),
                                                 request.model(),
                                                 COMPLETION,
                                                 ESTIMATED_PROMPT_TOKENS,
                                                 COMPLETION_TOKENS,
                                                 ENGINE_LATENCY,
                                                 CLOCK.instant()));
    }

    private void givenAllCollaboratorsCanComplete() {
        givenSeedCalculatorCanCalculate();
        givenAttemptTrackerCanCount();
        givenFaultPolicyCanPass();
        givenLatencyModelCanDraw();
        givenSynthesiserCanSynthesise();
        givenPromptTokenEstimatorCanEstimate();
    }

    private void givenFaultPolicyCanPass() {
        when(faults.evaluate(anyLong(), anyInt(), any())).thenReturn(Optional.empty());
    }

    private void givenLatencyModelCanDraw() {
        when(latency.drawCompletionTokens(anyLong(), anyInt())).thenReturn(COMPLETION_TOKENS);
        when(latency.draw(anyLong(), anyInt())).thenReturn(ENGINE_LATENCY);
    }

    private void givenSynthesiserCanSynthesise() {
        when(synthesiser.synthesise(any(), anyInt())).thenReturn(COMPLETION);
    }

    private void givenPromptTokenEstimatorCanEstimate() {
        when(promptTokens.estimate(any())).thenReturn(ESTIMATED_PROMPT_TOKENS);
    }

    @Test
    void drawsEveryRandomnessFromTheJobSeed() {
        final var givenRequest = givenRequest();
        givenAllCollaboratorsCanComplete();

        engine.run(givenRequest);

        verify(seeds).calculate(givenRequest.jobId());
        verify(faults).evaluate(SEED, ATTEMPT, givenRequest);
        verify(latency).drawCompletionTokens(SEED, MAX_TOKENS);
        verify(latency).draw(SEED, COMPLETION_TOKENS);
    }

    @Test
    void reportsTheAttemptCountToTheFaultPolicy() {
        final var givenRequest = givenRequest();
        givenAllCollaboratorsCanComplete();
        givenAttemptTrackerCanCountRetries();

        engine.run(givenRequest);

        verify(attempts).next(givenRequest.jobId());
        verify(faults).evaluate(SEED, RETRY_ATTEMPT, givenRequest);
    }

    private void givenAttemptTrackerCanCountRetries() {
        when(attempts.next(any())).thenReturn(RETRY_ATTEMPT);
    }

    @Test
    void synthesisesAsManyTokensAsTheLatencyModelDrew() {
        final var givenRequest = givenRequest();
        givenAllCollaboratorsCanComplete();

        engine.run(givenRequest);

        verify(synthesiser).synthesise(givenRequest, COMPLETION_TOKENS);
        verify(promptTokens).estimate(givenRequest.prompt());
    }

    @Test
    void delaysForTheLatencyItReports() {
        final var givenRequest = givenRequest();
        givenAllCollaboratorsCanComplete();

        engine.run(givenRequest);

        verify(delays).simulate(ENGINE_LATENCY);
    }

    @Test
    void leavesTheDelayUnsimulatedWhenThePolicyRejectsTheRequest() {
        final var givenRequest = givenRequest();
        givenSeedCalculatorCanCalculate();
        givenAttemptTrackerCanCount();
        givenFaultPolicyCanFailWithTimeout();

        engine.run(givenRequest);

        verify(delays, never()).simulate(any());
    }
}
