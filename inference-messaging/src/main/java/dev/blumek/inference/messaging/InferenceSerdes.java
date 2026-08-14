package dev.blumek.inference.messaging;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.InferenceResult;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public final class InferenceSerdes {

    private InferenceSerdes() {}

    public static Serializer<InferenceRequest> requestSerializer() {
        return new JacksonSerializer<>(InferenceRequest.class);
    }

    public static Deserializer<InferenceRequest> requestDeserializer() {
        return new JacksonDeserializer<>(InferenceRequest.class);
    }

    public static Serializer<InferenceResult> resultSerializer() {
        return new JacksonSerializer<>(InferenceResult.class);
    }

    public static Deserializer<InferenceResult> resultDeserializer() {
        return new JacksonDeserializer<>(InferenceResult.class);
    }
}
