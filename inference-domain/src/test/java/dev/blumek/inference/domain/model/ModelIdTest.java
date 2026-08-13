package dev.blumek.inference.domain.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class ModelIdTest {

    @ParameterizedTest
    @ValueSource(strings = {"llama3", "l", "0", "llama-3.1_8b", "llama3:8b", "a.b-c_d:1.0-rc_2"})
    void acceptsNameWithOptionalTag(String value) {
        assertThat(new ModelId(value).value()).isEqualTo(value);
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
    void rejectsIdOutsideAllowedShape(String value) {
        assertThatIllegalArgumentException().isThrownBy(() -> new ModelId(value));
    }

    @Test
    void rejectsNull() {
        assertThatNullPointerException().isThrownBy(() -> new ModelId(null));
    }

    @Test
    void reportsOffendingValue() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ModelId("Llama3"))
                .withMessageContaining("Llama3");
    }
}
