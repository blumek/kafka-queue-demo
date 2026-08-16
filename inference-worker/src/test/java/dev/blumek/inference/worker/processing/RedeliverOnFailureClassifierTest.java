package dev.blumek.inference.worker.processing;

import dev.blumek.inference.domain.model.InferenceResult;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.domain.port.EngineOutcome;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class RedeliverOnFailureClassifierTest {
    private static final Instant COMPLETED_AT = Instant.parse("2026-08-16T09:00:00Z");
    private static final int SECOND_DELIVERY = 2;

    private final RedeliverOnFailureClassifier classifier = new RedeliverOnFailureClassifier();

    @Test
    void acceptsAJobTheEngineCompleted() {
        final var actualDisposition = classifier.classify(givenACompletion(), SECOND_DELIVERY);

        assertThat(actualDisposition).isEqualTo(new Disposition.Accept());
    }

    @ParameterizedTest
    @MethodSource("everyWayTheEngineCanFail")
    void releasesAJobTheEngineCouldNotComplete(final EngineOutcome.Failed failure) {
        final var actualDisposition = classifier.classify(failure, SECOND_DELIVERY);

        assertThat(actualDisposition).isInstanceOf(Disposition.Release.class);
    }

    @ParameterizedTest
    @MethodSource("everyWayTheEngineCanFail")
    void saysWhichDeliveryFailedSoTheLogReadsBack(final EngineOutcome.Failed failure) {
        final var actualDisposition = classifier.classify(failure, SECOND_DELIVERY);

        assertThat(actualDisposition).isInstanceOfSatisfying(Disposition.Release.class,
                released -> assertThat(released.reason()).contains("delivery 2"));
    }

    private static Stream<EngineOutcome.Failed> everyWayTheEngineCanFail() {
        return Stream.of(
                new EngineOutcome.Failed.Timeout(Duration.ofSeconds(30)),
                new EngineOutcome.Failed.RateLimited(Optional.of(Duration.ofSeconds(5))),
                new EngineOutcome.Failed.MalformedInput("the prompt was empty"),
                new EngineOutcome.Failed.Unavailable("the engine refused the connection"));
    }

    private static EngineOutcome givenACompletion() {
        return new EngineOutcome.Completed(new InferenceResult(JobId.newId(), new ModelId("llama3:8b"),
                "because of Rayleigh scattering", 5, 6, Duration.ofMillis(120), COMPLETED_AT));
    }
}
