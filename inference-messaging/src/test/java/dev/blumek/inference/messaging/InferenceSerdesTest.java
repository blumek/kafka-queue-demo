package dev.blumek.inference.messaging;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.InferenceResult;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JacksonException;

import java.time.Duration;
import java.time.Instant;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class InferenceSerdesTest {
    private static final String TOPIC = InferenceTopics.JOBS;
    private static final Instant SUBMITTED_AT = Instant.parse("2026-08-14T10:15:30.123456789Z");

    @Test
    void roundTripsARequest() {
        final var givenRequest = givenRequest();

        final var actualBytes = InferenceSerdes.requestSerializer().serialize(TOPIC, givenRequest);

        assertThat(InferenceSerdes.requestDeserializer().deserialize(TOPIC, actualBytes)).isEqualTo(givenRequest);
    }

    private InferenceRequest givenRequest() {
        return new InferenceRequest(new JobId("job-42"),
                                    new ModelId("llama3:8b"),
                                    "why is the sky blue?",
                                    256,
                                    SUBMITTED_AT);
    }

    @Test
    void roundTripsAResult() {
        final var givenResult = givenResult();

        final var actualBytes = InferenceSerdes.resultSerializer().serialize(InferenceTopics.RESULTS, givenResult);

        assertThat(InferenceSerdes.resultDeserializer().deserialize(InferenceTopics.RESULTS, actualBytes))
                .isEqualTo(givenResult);
    }

    private InferenceResult givenResult() {
        return new InferenceResult(new JobId("job-42"),
                                   new ModelId("llama3:8b"),
                                   "[sim llama3:8b] because of rayleigh scattering",
                                   5,
                                   7,
                                   Duration.ofNanos(7_000_123),
                                   Instant.parse("2026-08-14T10:15:31.987654321Z"));
    }

    @Test
    void writesTheIdentifiersAsFlatStrings() {
        final var actualJson = new String(
                InferenceSerdes.requestSerializer().serialize(TOPIC, givenRequest()), UTF_8);

        assertThat(actualJson).contains("\"jobId\":\"job-42\"", "\"model\":\"llama3:8b\"");
        assertThat(actualJson).contains("\"submittedAt\":\"2026-08-14T10:15:30.123456789Z\"");
    }

    @Test
    void toleratesAnUnknownFieldOnTheWire() {
        final var givenBytes = givenRequestJsonWithAnUnknownField();

        final var actualRequest = InferenceSerdes.requestDeserializer().deserialize(TOPIC, givenBytes);

        assertThat(actualRequest).isEqualTo(givenRequest());
    }

    private byte[] givenRequestJsonWithAnUnknownField() {
        return ("""
                {"jobId":"job-42","model":"llama3:8b","prompt":"why is the sky blue?",\
                "maxTokens":256,"submittedAt":"2026-08-14T10:15:30.123456789Z","priority":5}""").getBytes(UTF_8);
    }

    @Test
    void serialisesANullRequestAsNull() {
        assertThat(InferenceSerdes.requestSerializer().serialize(TOPIC, null)).isNull();
    }

    @Test
    void serialisesANullResultAsNull() {
        assertThat(InferenceSerdes.resultSerializer().serialize(InferenceTopics.RESULTS, null)).isNull();
    }

    @Test
    void deserialisesNullBytesAsNull() {
        assertThat(InferenceSerdes.requestDeserializer().deserialize(TOPIC, null)).isNull();
    }

    @Test
    void rejectsMalformedJson() {
        final var actualThrown = catchThrowable(
                () -> InferenceSerdes.requestDeserializer().deserialize(TOPIC, "{ not json".getBytes(UTF_8)));

        assertThat(actualThrown).isInstanceOf(SerializationException.class);
    }

    @Test
    void rejectsAnEmptyPayload() {
        final var actualThrown = catchThrowable(
                () -> InferenceSerdes.requestDeserializer().deserialize(TOPIC, new byte[0]));

        assertThat(actualThrown).isInstanceOf(SerializationException.class);
    }

    @Test
    void rejectsAPayloadWithAnInvalidModelId() {
        final var givenBytes = "{\"jobId\":\"job-42\",\"model\":\"Llama3\"}".getBytes(UTF_8);

        final var actualThrown = catchThrowable(
                () -> InferenceSerdes.requestDeserializer().deserialize(TOPIC, givenBytes));

        assertThat(actualThrown).isInstanceOf(SerializationException.class);
    }

    @Test
    void rejectsAPayloadMissingARequiredField() {
        final var givenBytes = "{\"jobId\":\"job-42\"}".getBytes(UTF_8);

        final var actualThrown = catchThrowable(
                () -> InferenceSerdes.requestDeserializer().deserialize(TOPIC, givenBytes));

        assertThat(actualThrown).isInstanceOf(SerializationException.class);
    }

    @Test
    void namesTheTypeAndTopicWhenDeserialisationFails() {
        final var actualThrown = catchThrowable(
                () -> InferenceSerdes.requestDeserializer().deserialize(TOPIC, "{ not json".getBytes(UTF_8)));

        assertThat(actualThrown).hasMessageContaining("InferenceRequest").hasMessageContaining(TOPIC);
    }

    @Test
    void keepsTheJacksonCauseWhenDeserialisationFails() {
        final var actualThrown = catchThrowable(
                () -> InferenceSerdes.requestDeserializer().deserialize(TOPIC, "{ not json".getBytes(UTF_8)));

        assertThat(actualThrown).hasCauseInstanceOf(JacksonException.class);
    }
}
