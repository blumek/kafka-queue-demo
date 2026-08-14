package dev.blumek.inference.messaging;

import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.ValueDeserializer;
import tools.jackson.databind.ValueSerializer;
import tools.jackson.databind.module.SimpleModule;

final class DomainIdsModule extends SimpleModule {

    DomainIdsModule() {
        addSerializer(JobId.class, new JobIdSerializer());
        addDeserializer(JobId.class, new JobIdDeserializer());
        addSerializer(ModelId.class, new ModelIdSerializer());
        addDeserializer(ModelId.class, new ModelIdDeserializer());
    }

    private static String stringValue(final JsonParser parser,
                                      final DeserializationContext context,
                                      final Class<?> target) {
        if (parser.currentToken() != JsonToken.VALUE_STRING) {
            return context.reportInputMismatch(target, "expected a JSON string but got %s", parser.currentToken());
        }
        return parser.getString();
    }

    private static final class JobIdSerializer extends ValueSerializer<JobId> {
        @Override
        public void serialize(final JobId value, final JsonGenerator generator, final SerializationContext context) {
            generator.writeString(value.value());
        }
    }

    private static final class JobIdDeserializer extends ValueDeserializer<JobId> {
        @Override
        public JobId deserialize(final JsonParser parser, final DeserializationContext context) {
            return new JobId(stringValue(parser, context, JobId.class));
        }
    }

    private static final class ModelIdSerializer extends ValueSerializer<ModelId> {
        @Override
        public void serialize(final ModelId value, final JsonGenerator generator, final SerializationContext context) {
            generator.writeString(value.value());
        }
    }

    private static final class ModelIdDeserializer extends ValueDeserializer<ModelId> {
        @Override
        public ModelId deserialize(final JsonParser parser, final DeserializationContext context) {
            final var value = stringValue(parser, context, ModelId.class);
            try {
                return new ModelId(value);
            } catch (final IllegalArgumentException e) {
                return context.reportInputMismatch(ModelId.class, "invalid model id: %s", value);
            }
        }
    }
}
