package dev.blumek.inference.messaging;

import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.exc.MismatchedInputException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class DomainIdsModuleTest {
    private static final String JOB_ID = "job-42";
    private static final String MODEL = "llama3:8b";

    @Test
    void writesAJobIdAsAFlatString() {
        final var givenJobId = new JobId(JOB_ID);

        final var actualJson = InferenceJson.mapper().writeValueAsString(givenJobId);

        assertThat(actualJson).isEqualTo("\"" + JOB_ID + "\"");
    }

    @Test
    void readsAJobIdFromAFlatString() {
        final var actualJobId = InferenceJson.mapper().readValue("\"" + JOB_ID + "\"", JobId.class);

        assertThat(actualJobId).isEqualTo(new JobId(JOB_ID));
    }

    @Test
    void writesAModelIdAsAFlatString() {
        final var givenModel = new ModelId(MODEL);

        final var actualJson = InferenceJson.mapper().writeValueAsString(givenModel);

        assertThat(actualJson).isEqualTo("\"" + MODEL + "\"");
    }

    @Test
    void readsAModelIdFromAFlatString() {
        final var actualModel = InferenceJson.mapper().readValue("\"" + MODEL + "\"", ModelId.class);

        assertThat(actualModel).isEqualTo(new ModelId(MODEL));
    }

    @Test
    void rejectsAModelIdOutsideTheAllowedShape() {
        final var actualThrown = catchThrowable(
                () -> InferenceJson.mapper().readValue("\"" + givenInvalidModel() + "\"", ModelId.class));

        assertThat(actualThrown).isInstanceOf(MismatchedInputException.class);
    }

    private String givenInvalidModel() {
        return "Llama3";
    }

    @Test
    void namesTheOffendingModelIdWhenItIsRejected() {
        final var givenInvalidModel = givenInvalidModel();

        final var actualThrown = catchThrowable(
                () -> InferenceJson.mapper().readValue("\"" + givenInvalidModel + "\"", ModelId.class));

        assertThat(actualThrown).hasMessageContaining(givenInvalidModel);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"value\":\"" + JOB_ID + "\"}", "42", "true", "[\"" + JOB_ID + "\"]"})
    void rejectsAJobIdThatIsNotAString(final String givenJson) {
        final var actualThrown = catchThrowable(() -> InferenceJson.mapper().readValue(givenJson, JobId.class));

        assertThat(actualThrown).isInstanceOf(MismatchedInputException.class);
    }

    @Test
    void readsAJobIdWrittenByTheSerializer() {
        final var givenJobId = new JobId(JOB_ID);

        final var actualJobId = InferenceJson.mapper()
                .readValue(InferenceJson.mapper().writeValueAsString(givenJobId), JobId.class);

        assertThat(actualJobId).isEqualTo(givenJobId);
    }
}
