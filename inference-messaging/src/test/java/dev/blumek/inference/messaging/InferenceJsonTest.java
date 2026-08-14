package dev.blumek.inference.messaging;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class InferenceJsonTest {
    private static final Instant NANOSECOND_PRECISE = Instant.parse("2026-08-14T10:15:30.123456789Z");

    @Test
    void buildsTheMapperOnce() {
        assertThat(InferenceJson.mapper()).isSameAs(InferenceJson.mapper());
    }

    @Test
    void writesInstantsAsIsoStrings() {
        final var givenInstant = Instant.parse("2026-08-14T10:15:30Z");

        final var actualJson = InferenceJson.mapper().writeValueAsString(givenInstant);

        assertThat(actualJson).isEqualTo("\"2026-08-14T10:15:30Z\"");
    }

    @Test
    void keepsSubSecondInstantPrecision() {
        final var actualJson = InferenceJson.mapper().writeValueAsString(NANOSECOND_PRECISE);

        assertThat(actualJson).contains(".123456789");
        assertThat(InferenceJson.mapper().readValue(actualJson, Instant.class)).isEqualTo(NANOSECOND_PRECISE);
    }

    @Test
    void writesDurationsAsIsoStrings() {
        final var givenDuration = Duration.ofMillis(7);

        final var actualJson = InferenceJson.mapper().writeValueAsString(givenDuration);

        assertThat(actualJson).isEqualTo("\"PT0.007S\"");
    }

    @Test
    void keepsSubSecondDurationPrecision() {
        final var givenDuration = Duration.ofNanos(1);

        final var actualJson = InferenceJson.mapper().writeValueAsString(givenDuration);

        assertThat(InferenceJson.mapper().readValue(actualJson, Duration.class)).isEqualTo(givenDuration);
    }
}
