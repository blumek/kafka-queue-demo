package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.port.InferenceEngine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.time.Clock;

@Configuration
class SimulatorConfiguration {

    private static final String RUN_SEED = "inference.simulator.run-seed";

    @Bean
    Seeds seeds() {
        return new Seeds();
    }

    @Bean
    FaultPolicy faultPolicy(final Seeds seeds) {
        return FaultPolicy.defaults(seeds);
    }

    @Bean
    LatencyModel latencyModel(final Seeds seeds) {
        return LatencyModel.defaults(seeds);
    }

    @Bean
    DelaySimulator delaySimulator() {
        return new DelaySimulator();
    }

    @Bean
    CompletionSynthesiser completionSynthesiser() {
        return new CompletionSynthesiser();
    }

    @Bean
    SeedCalculator seedCalculator(final Environment environment) {
        return new SeedCalculator(environment.getProperty(RUN_SEED, Long.class, 0L));
    }

    @Bean
    PromptTokenEstimator promptTokenEstimator() {
        return new PromptTokenEstimator();
    }

    @Bean
    AttemptTracker attemptTracker() {
        return new AttemptTracker();
    }

    @Bean
    InferenceEngine simulatedEngine(final FaultPolicy faults,
                                    final LatencyModel latency,
                                    final DelaySimulator delays,
                                    final CompletionSynthesiser synthesiser,
                                    final SeedCalculator seeds,
                                    final PromptTokenEstimator promptTokens,
                                    final AttemptTracker attempts) {
        return new SimulatedInferenceEngine(faults, latency, delays, synthesiser, seeds, promptTokens, attempts, Clock.systemUTC());
    }
}
