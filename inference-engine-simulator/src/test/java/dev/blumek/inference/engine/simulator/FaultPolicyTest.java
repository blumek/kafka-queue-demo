package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.domain.port.EngineOutcome.Failed;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FaultPolicyTest {
    private static final long SEED = 1L;
    private static final int ATTEMPT = 1;
    private static final int RETRY_ATTEMPT = 7;
    private static final String JOB_ID = "job-1";
    private static final String MODEL = "llama3:8b";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final Seeds seeds = mock(Seeds.class);

    private final FaultPolicy policy = FaultPolicy.defaults(seeds);

    @Test
    void passesTheRequestThroughWhenNoRateIsMet() {
        givenSeedsCanDraw(1.0);

        final var actualFault = policy.evaluate(SEED, ATTEMPT, givenRequest());

        assertThat(actualFault).isEmpty();
    }

    private void givenSeedsCanDraw(final double draw) {
        when(seeds.nextDouble(anyLong(), anyLong())).thenReturn(draw);
        when(seeds.nextDouble(anyLong(), anyLong(), anyLong())).thenReturn(draw);
    }

    private InferenceRequest givenRequest() {
        return new InferenceRequest(new JobId(JOB_ID), new ModelId(MODEL), "why is the sky blue?", 256, Instant.EPOCH);
    }

    @Test
    void passesTheRequestThroughWhenTheDrawOnlyMeetsTheRate() {
        final var givenPolicy = givenPolicy(0.5, 0.5, 0.5, 0.5);
        givenSeedsCanDraw(0.5);

        final var actualFault = givenPolicy.evaluate(SEED, ATTEMPT, givenRequest());

        assertThat(actualFault).isEmpty();
    }

    private FaultPolicy givenPolicy(final double transientRate,
                                    final double rateLimitedRate,
                                    final double malformedRate,
                                    final double alwaysFailsRate) {
        return new FaultPolicy(transientRate, rateLimitedRate, malformedRate, alwaysFailsRate, TIMEOUT, seeds);
    }

    @Test
    void rejectsThePromptWhenTheDrawFallsUnderTheMalformedRate() {
        final var givenPolicy = givenPolicy(0, 0, 1, 0);
        givenSeedsCanDraw(0.0);

        final var actualFault = givenPolicy.evaluate(SEED, ATTEMPT, givenRequest());

        assertThat(actualFault).containsInstanceOf(Failed.MalformedInput.class);
    }

    @Test
    void namesTheOffendingJobWhenTheDrawFallsUnderTheAlwaysFailsRate() {
        final var givenPolicy = givenPolicy(0, 0, 0, 1);
        givenSeedsCanDraw(0.0);

        final var actualFault = givenPolicy.evaluate(SEED, ATTEMPT, givenRequest());

        assertThat(actualFault).get()
                .isInstanceOfSatisfying(Failed.Unavailable.class,
                        unavailable -> assertThat(unavailable.detail()).contains(JOB_ID, MODEL));
    }

    @Test
    void rateLimitsWhenTheDrawFallsUnderTheRateLimitedRate() {
        final var givenPolicy = givenPolicy(0, 1, 0, 0);
        givenSeedsCanDraw(0.0);

        final var actualFault = givenPolicy.evaluate(SEED, ATTEMPT, givenRequest());

        assertThat(actualFault).contains(new Failed.RateLimited(Optional.of(Duration.ofMillis(250))));
    }

    @Test
    void backsOffFurtherTheLongerRateLimitingPersists() {
        final var givenPolicy = givenPolicy(0, 1, 0, 0);
        givenSeedsCanDraw(0.0);

        final var actualFault = givenPolicy.evaluate(SEED, 4, givenRequest());

        assertThat(actualFault).contains(new Failed.RateLimited(Optional.of(Duration.ofSeconds(1))));
    }

    @Test
    void timesOutForTheConfiguredBudgetWhenTheDrawFallsUnderTheTransientRate() {
        final var givenPolicy = givenPolicy(1, 0, 0, 0);
        givenSeedsCanDraw(0.0);

        final var actualFault = givenPolicy.evaluate(SEED, ATTEMPT, givenRequest());

        assertThat(actualFault).contains(new Failed.Timeout(TIMEOUT));
    }

    @Test
    void prefersTheMalformedFaultOverEveryOtherKind() {
        final var givenPolicy = givenPolicy(1, 1, 1, 1);
        givenSeedsCanDraw(0.0);

        final var actualFault = givenPolicy.evaluate(SEED, ATTEMPT, givenRequest());

        assertThat(actualFault).containsInstanceOf(Failed.MalformedInput.class);
    }

    @Test
    void keepsIntrinsicFaultsBlindToTheAttempt() {
        givenSeedsCanDraw(1.0);

        policy.evaluate(SEED, RETRY_ATTEMPT, givenRequest());

        verify(seeds, times(2)).nextDouble(eq(SEED), anyLong());
    }

    @Test
    void feedsTheAttemptIntoTransientFaultDraws() {
        givenSeedsCanDraw(1.0);

        policy.evaluate(SEED, RETRY_ATTEMPT, givenRequest());

        verify(seeds, times(2)).nextDouble(eq(SEED), anyLong(), eq((long) RETRY_ATTEMPT));
    }

    @ParameterizedTest
    @ValueSource(doubles = {-0.001, 1.001})
    void rejectsRateOutsideUnitRange(final double givenRate) {
        final var actualThrown = catchThrowable(() -> givenPolicy(givenRate, 0, 0, 0));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("transientRate");
    }

    @ParameterizedTest
    @ValueSource(doubles = {-0.001, 1.001})
    void rejectsRateOutsideUnitRangeForEveryFaultKind(final double givenRate) {
        assertThat(catchThrowable(() -> givenPolicy(0, givenRate, 0, 0))).isInstanceOf(IllegalArgumentException.class);
        assertThat(catchThrowable(() -> givenPolicy(0, 0, givenRate, 0))).isInstanceOf(IllegalArgumentException.class);
        assertThat(catchThrowable(() -> givenPolicy(0, 0, 0, givenRate))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveTimeoutBudget() {
        final var actualThrown = catchThrowable(() -> new FaultPolicy(0, 0, 0, 0, Duration.ZERO, seeds));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullTimeoutBudget() {
        final var actualThrown = catchThrowable(() -> new FaultPolicy(0, 0, 0, 0, null, seeds));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectsNonPositiveAttempt(final int givenAttempt) {
        final var actualThrown = catchThrowable(() -> policy.evaluate(SEED, givenAttempt, givenRequest()));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }
}
