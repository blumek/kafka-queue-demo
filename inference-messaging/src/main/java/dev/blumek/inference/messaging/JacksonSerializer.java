package dev.blumek.inference.messaging;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import tools.jackson.core.JacksonException;

final class JacksonSerializer<T> implements Serializer<T> {
    private final Class<T> type;

    JacksonSerializer(final Class<T> type) {
        this.type = type;
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data == null) {
            return null;
        }

        try {
            return InferenceJson.mapper().writeValueAsBytes(data);
        } catch (final JacksonException e) {
            throw new SerializationException(
                    "failed to serialize " + type.getSimpleName() + " for topic " + topic, e);
        }
    }
}
