package dev.blumek.inference.domain.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class ModelIdTest {

    @ParameterizedTest
    @ValueSource(strings = {"llama3", "l", "0", "llama-3.1_8b", "llama3:8b", "a.b-c_d:1.0-rc_2"})
    void acceptsNameWithOptionalTag(final String givenValue) {
        final var actualModelId = new ModelId(givenValue);

        assertThat(actualModelId.value()).isEqualTo(givenValue);
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {
            "Llama3",
            "llama3:8B",
            "llama 3",
            "llama3:",
            ":8b",
            ":",
            "llama3:8b:q4",
            "llama3/8b",
            "llama3@latest",
            " llama3",
            "llama3\n"
    })
    void rejectsIdOutsideAllowedShape(final String givenValue) {
        final var actualThrown = catchThrowable(() -> new ModelId(givenValue));

        assertThat(actualThrown).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNull() {
        final var actualThrown = catchThrowable(() -> new ModelId(null));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void reportsOffendingValue() {
        final var givenInvalidValue = givenInvalidValue();

        final var actualThrown = catchThrowable(() -> new ModelId(givenInvalidValue));

        assertThat(actualThrown).hasMessageContaining(givenInvalidValue);
    }

    private String givenInvalidValue() {
        return "Llama3";
    }
}
