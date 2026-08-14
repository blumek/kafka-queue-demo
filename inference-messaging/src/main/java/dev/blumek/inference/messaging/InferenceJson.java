package dev.blumek.inference.messaging;

import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

final class InferenceJson {
    private static final JsonMapper MAPPER = JsonMapper.builder()
            .addModule(new DomainIdsModule())
            .disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DateTimeFeature.WRITE_DURATIONS_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private InferenceJson() {}

    static JsonMapper mapper() {
        return MAPPER;
    }
}
