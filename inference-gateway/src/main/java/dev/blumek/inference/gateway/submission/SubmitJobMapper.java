package dev.blumek.inference.gateway.submission;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;

import java.time.Clock;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

class SubmitJobMapper {
    private final Clock clock;

    SubmitJobMapper(final Clock clock) {
        this.clock = clock;
    }

    InferenceRequest toInferenceRequest(final SubmissionCommand command) {
        requireNonNull(command);
        return new InferenceRequest(getJobId(command),
                new ModelId(command.model()),
                command.prompt(),
                command.maxTokens(),
                clock.instant());
    }

    private static JobId getJobId(final SubmissionCommand command) {
        return command.idempotencyKey()
                .map(SubmitJobMapper::jobIdFrom)
                .orElseGet(JobId::newId);
    }

    private static JobId jobIdFrom(final String idempotencyKey) {
        try {
            return new JobId(UUID.fromString(idempotencyKey.strip()).toString());
        } catch (final IllegalArgumentException exception) {
            throw new MalformedIdempotencyKeyException(exception);
        }
    }
}
