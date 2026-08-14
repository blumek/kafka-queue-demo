package dev.blumek.inference.engine.simulator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LatencyModelTest {
    private static final long SEED = 42L;
    private static final int CEILING = 400;
    private static final int TOKENS = 100;

    private final Seeds seeds = mock(Seeds.class);

    private final LatencyModel model = LatencyModel.defaults(seeds);

    @Test
    void drawsTheFloorFractionOfTheCeilingWhenTheDrawIsLowest() {
        givenSeedsCanDraw(0.0);

        final var actualTokens = model.drawCompletionTokens(SEED, CEILING);

        assertThat(actualTokens).isEqualTo(60);
    }

    private void givenSeedsCanDraw(final double fraction) {
        when(seeds.nextDouble(anyLong(), anyLong())).thenReturn(fraction);
    }

    @Test
    void spendsTheWholeCeilingWhenTheDrawIsHighest() {
        givenSeedsCanDraw(1.0);

        final var actualTokens = model.drawCompletionTokens(SEED, CEILING);

        assertThat(actualTokens).isEqualTo(CEILING);
    }

    @Test
    void interpolatesBetweenTheConfiguredFractions() {
        givenSeedsCanDraw(0.5);

        final var actualTokens = model.drawCompletionTokens(SEED, CEILING);

        assertThat(actualTokens).isEqualTo(230);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 7})
    void neverDrawsFewerThanOneToken(final int givenMaxTokens) {
        givenSeedsCanDraw(0.0);

        final var actualTokens = model.drawCompletionTokens(SEED, givenMaxTokens);

        assertThat(actualTokens).isEqualTo(1);
    }

    @Test
    void addsTheMedianGenerationCostOnTopOfTheOverhead() {
        givenSeedsCanDrawGaussian(0.0);

        final var actualLatency = model.draw(SEED, TOKENS);

        assertThat(actualLatency).isEqualTo(Duration.ofMillis(1_920));
    }

    private void givenSeedsCanDrawGaussian(final double gaussian) {
        when(seeds.nextGaussian(anyLong(), anyLong())).thenReturn(gaussian);
    }

    @Test
    void scalesLatencyWithTokensGenerated() {
        givenSeedsCanDrawGaussian(0.0);

        final var actualLatency = model.draw(SEED, CEILING);

        assertThat(actualLatency).isGreaterThan(model.draw(SEED, TOKENS));
    }

    @Test
    void spreadsLatencyIntoATail() {
        givenSeedsCanDrawGaussians(1.0, 0.0);

        final var actualLatency = model.draw(SEED, TOKENS);

        assertThat(actualLatency).isGreaterThan(model.draw(SEED, TOKENS));
    }

    private void givenSeedsCanDrawGaussians(final double first, final double second) {
        when(seeds.nextGaussian(anyLong(), anyLong())).thenReturn(first, second);
    }

    @ParameterizedTest
    @CsvSource({"10.0, 3.0", "-10.0, -3.0"})
    void clampsTheGaussianToThreeSigma(final double givenGaussian, final double givenClamped) {
        givenSeedsCanDrawGaussians(givenGaussian, givenClamped);

        final var actualLatency = model.draw(SEED, TOKENS);

        assertThat(actualLatency).isEqualTo(model.draw(SEED, TOKENS));
    }

    @Test
    void neverUndercutsOverhead() {
        givenSeedsCanDrawGaussian(-3.0);

        final var actualLatency = model.draw(SEED, TOKENS);

        assertThat(actualLatency).isGreaterThan(model.overhead());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectsNonPositiveCeiling(final int givenMaxTokens) {
        final var actualThrown = catchThrowable(() -> model.drawCompletionTokens(SEED, givenMaxTokens));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void rejectsNonPositiveTokenCount(final int givenCompletionTokens) {
        final var actualThrown = catchThrowable(() -> model.draw(SEED, givenCompletionTokens));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNegativeOverhead() {
        final var actualThrown = catchThrowable(
                () -> new LatencyModel(Duration.ofMillis(-1), Duration.ofMillis(18), 0.35, 0.15, 1.0, seeds));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositivePerToken() {
        final var actualThrown = catchThrowable(
                () -> new LatencyModel(Duration.ZERO, Duration.ZERO, 0.35, 0.15, 1.0, seeds));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveSigma() {
        final var actualThrown = catchThrowable(
                () -> new LatencyModel(Duration.ZERO, Duration.ofMillis(18), 0.0, 0.15, 1.0, seeds));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(doubles = {0.0, 1.5})
    void rejectsFractionOutsideUnitRange(final double givenMinFraction) {
        final var actualThrown = catchThrowable(
                () -> new LatencyModel(Duration.ZERO, Duration.ofMillis(18), 0.35, givenMinFraction, 1.0, seeds));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsInvertedFractionRange() {
        final var actualThrown = catchThrowable(
                () -> new LatencyModel(Duration.ZERO, Duration.ofMillis(18), 0.35, 0.8, 0.2, seeds));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }
}
