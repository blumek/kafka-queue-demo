package dev.blumek.inference.engine.simulator;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class SeedsTest {
    private static final long SEED = 42L;
    private static final long PURPOSE = 1L;
    private static final int DRAWS = 20_000;

    private final Seeds seeds = new Seeds();

    @Test
    void drawsTheSameValueForTheSameParts() {
        final var actualDraw = seeds.nextDouble(SEED, PURPOSE);

        assertThat(actualDraw).isEqualTo(new Seeds().nextDouble(SEED, PURPOSE));
    }

    @Test
    void drawsTheSameGaussianForTheSameParts() {
        final var actualDraw = seeds.nextGaussian(SEED, PURPOSE);

        assertThat(actualDraw).isEqualTo(new Seeds().nextGaussian(SEED, PURPOSE));
    }

    @Test
    void separatesPurposesSharingASeed() {
        final var actualDraw = seeds.nextDouble(SEED, PURPOSE);

        assertThat(actualDraw).isNotEqualTo(seeds.nextDouble(SEED, PURPOSE + 1));
    }

    @Test
    void separatesSeedsSharingAPurpose() {
        final var actualDraw = seeds.nextDouble(SEED, PURPOSE);

        assertThat(actualDraw).isNotEqualTo(seeds.nextDouble(SEED + 1, PURPOSE));
    }

    @Test
    void isSensitiveToPartOrder() {
        final var actualDraw = seeds.nextDouble(SEED, PURPOSE);

        assertThat(actualDraw).isNotEqualTo(seeds.nextDouble(PURPOSE, SEED));
    }

    @Test
    void staysWithinTheUnitRange() {
        final var actualDraws = givenDoubleDraws();

        assertThat(actualDraws).allSatisfy(draw -> assertThat(draw).isBetween(0.0, 1.0));
    }

    private List<Double> givenDoubleDraws() {
        return IntStream.range(0, DRAWS).mapToObj(seeds::nextDouble).toList();
    }

    @Test
    void spreadsUniformlyAcrossTheUnitRange() {
        final var actualDraws = givenDoubleDraws();

        assertThat(meanOf(actualDraws)).isBetween(0.48, 0.52);
    }

    private double meanOf(final List<Double> draws) {
        return draws.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
    }

    @Test
    void centresGaussianDrawsOnZero() {
        final var actualDraws = IntStream.range(0, DRAWS).mapToObj(seeds::nextGaussian).toList();

        assertThat(meanOf(actualDraws)).isBetween(-0.03, 0.03);
    }
}
