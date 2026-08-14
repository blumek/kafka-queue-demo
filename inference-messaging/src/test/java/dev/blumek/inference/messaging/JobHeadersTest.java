package dev.blumek.inference.messaging;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.format.DateTimeParseException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class JobHeadersTest {
    private static final Instant NOT_BEFORE = Instant.parse("2026-08-14T10:15:30.123456789Z");
    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Test
    void writesTheNotBeforeDeadlineAsAnIsoString() {
        final var givenHeaders = new RecordHeaders();

        JobHeaders.putNotBefore(givenHeaders, NOT_BEFORE);

        assertThat(new String(givenHeaders.lastHeader(JobHeaders.NOT_BEFORE).value(), UTF_8))
                .isEqualTo("2026-08-14T10:15:30.123456789Z");
    }

    @Test
    void readsBackTheNotBeforeDeadline() {
        final var givenHeaders = new RecordHeaders();
        JobHeaders.putNotBefore(givenHeaders, NOT_BEFORE);

        final var actualNotBefore = JobHeaders.notBefore(givenHeaders);

        assertThat(actualNotBefore).contains(NOT_BEFORE);
    }

    @Test
    void readsAnAbsentNotBeforeAsEmpty() {
        final var actualNotBefore = JobHeaders.notBefore(new RecordHeaders());

        assertThat(actualNotBefore).isEmpty();
    }

    @Test
    void replacesAnExistingNotBeforeInsteadOfAppending() {
        final var givenHeaders = new RecordHeaders();
        JobHeaders.putNotBefore(givenHeaders, NOT_BEFORE);

        JobHeaders.putNotBefore(givenHeaders, NOT_BEFORE.plusSeconds(30));

        assertThat(givenHeaders.headers(JobHeaders.NOT_BEFORE)).hasSize(1);
        assertThat(JobHeaders.notBefore(givenHeaders)).contains(NOT_BEFORE.plusSeconds(30));
    }

    @Test
    void rejectsAMalformedNotBefore() {
        final var givenHeaders = new RecordHeaders();
        givenHeaders.add(JobHeaders.NOT_BEFORE, "soon".getBytes(UTF_8));

        final var actualThrown = catchThrowable(() -> JobHeaders.notBefore(givenHeaders));

        assertThat(actualThrown).isInstanceOf(DateTimeParseException.class);
    }

    @Test
    void writesTheTraceparentVerbatim() {
        final var givenHeaders = new RecordHeaders();

        JobHeaders.putTraceparent(givenHeaders, TRACEPARENT);

        assertThat(new String(givenHeaders.lastHeader(JobHeaders.TRACEPARENT).value(), UTF_8))
                .isEqualTo(TRACEPARENT);
    }

    @Test
    void readsBackTheTraceparent() {
        final var givenHeaders = new RecordHeaders();
        JobHeaders.putTraceparent(givenHeaders, TRACEPARENT);

        final var actualTraceparent = JobHeaders.traceparent(givenHeaders);

        assertThat(actualTraceparent).contains(TRACEPARENT);
    }

    @Test
    void readsAnAbsentTraceparentAsEmpty() {
        final var actualTraceparent = JobHeaders.traceparent(new RecordHeaders());

        assertThat(actualTraceparent).isEmpty();
    }

    @Test
    void replacesAnExistingTraceparentInsteadOfAppending() {
        final var givenHeaders = new RecordHeaders();
        JobHeaders.putTraceparent(givenHeaders, TRACEPARENT);

        JobHeaders.putTraceparent(givenHeaders, TRACEPARENT);

        assertThat(givenHeaders.headers(JobHeaders.TRACEPARENT)).hasSize(1);
    }

    @Test
    void rejectsNullArguments() {
        final var givenHeaders = new RecordHeaders();

        assertThat(catchThrowable(() -> JobHeaders.putNotBefore(givenHeaders, null)))
                .isInstanceOf(NullPointerException.class);
        assertThat(catchThrowable(() -> JobHeaders.putTraceparent(givenHeaders, null)))
                .isInstanceOf(NullPointerException.class);
        assertThat(catchThrowable(() -> JobHeaders.notBefore(null)))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void usesTheW3cSpellingForTheTraceparentKey() {
        assertThat(JobHeaders.TRACEPARENT).isEqualTo("traceparent");
    }
}
