package dev.blumek.inference.messaging;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import tools.jackson.core.JacksonException;

final class JacksonDeserializer<T> implements Deserializer<T> {
    private final Class<T> type;

    JacksonDeserializer(final Class<T> type) {
        this.type = type;
    }

    @Override
    public T deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return InferenceJson.mapper().readValue(data, type);
        } catch (final JacksonException e) {
            throw new SerializationException(
                    "failed to deserialize " + type.getSimpleName() + " from topic " + topic, e);
        }
    }
}
