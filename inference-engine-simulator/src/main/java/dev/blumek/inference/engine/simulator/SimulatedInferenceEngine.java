package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.InferenceResult;
import dev.blumek.inference.domain.port.EngineOutcome;
import dev.blumek.inference.domain.port.EngineOutcome.Completed;
import dev.blumek.inference.domain.port.InferenceEngine;

import java.time.Clock;
import java.time.Duration;

final class SimulatedInferenceEngine implements InferenceEngine {
    private final FaultPolicy faults;
    private final LatencyModel latency;
    private final DelaySimulator delays;
    private final CompletionSynthesiser synthesiser;
    private final SeedCalculator seeds;
    private final PromptTokenEstimator promptTokens;
    private final AttemptTracker attempts;
    private final Clock clock;

    SimulatedInferenceEngine(final FaultPolicy faults,
                             final LatencyModel latency,
                             final DelaySimulator delays,
                             final CompletionSynthesiser synthesiser,
                             final SeedCalculator seeds,
                             final PromptTokenEstimator promptTokens,
                             final AttemptTracker attempts,
                             final Clock clock) {
        this.faults = faults;
        this.latency = latency;
        this.delays = delays;
        this.synthesiser = synthesiser;
        this.seeds = seeds;
        this.promptTokens = promptTokens;
        this.attempts = attempts;
        this.clock = clock;
    }

    @Override
    public EngineOutcome run(final InferenceRequest request) {
        var seed = seeds.calculate(request.jobId());

        return faults.evaluate(seed, attempts.next(request.jobId()), request)
                .map(fault -> (EngineOutcome) fault)
                .orElseGet(() -> simulatedCompletion(request, seed));
    }

    private Completed simulatedCompletion(final InferenceRequest request, final long seed) {
        var completionTokens = latency.drawCompletionTokens(seed, request.maxTokens());
        var elapsed = latency.draw(seed, completionTokens);
        delays.simulate(elapsed);

        return new Completed(toInferenceResult(request, completionTokens, elapsed));
    }

    private InferenceResult toInferenceResult(final InferenceRequest request,
                                              final int completionTokens,
                                              final Duration elapsed) {
        return new InferenceResult(
                request.jobId(),
                request.model(),
                synthesiser.synthesise(request, completionTokens),
                promptTokens.estimate(request.prompt()),
                completionTokens,
                elapsed,
                clock.instant());
    }
}
