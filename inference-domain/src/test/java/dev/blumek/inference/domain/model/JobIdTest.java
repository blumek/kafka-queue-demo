package dev.blumek.inference.domain.model;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

class JobIdTest {
    private static final int GENERATED_IDS = 1_000;

    @Test
    void keepsGivenValue() {
        final var givenValue = givenValue();

        final var actualJobId = new JobId(givenValue);

        assertThat(actualJobId.value()).isEqualTo(givenValue);
    }

    private String givenValue() {
        return "job-42";
    }

    @Test
    void rejectsNull() {
        final var actualThrown = catchThrowable(() -> new JobId(null));

        assertThat(actualThrown).isInstanceOf(NullPointerException.class);
    }

    @Test
    void equalsByValue() {
        final var givenValue = givenValue();

        final var actualJobId = new JobId(givenValue);

        assertThat(actualJobId).isEqualTo(new JobId(givenValue));
        assertThat(actualJobId).isNotEqualTo(new JobId(givenOtherValue()));
    }

    private String givenOtherValue() {
        return "job-43";
    }

    @Test
    void newIdIsAUuid() {
        final var actualJobId = JobId.newId();

        assertThatCode(() -> UUID.fromString(actualJobId.value())).doesNotThrowAnyException();
    }

    @Test
    void newIdIsUniquePerCall() {
        final var actualIds = Stream.generate(JobId::newId)
                .limit(GENERATED_IDS)
                .collect(HashSet::new, Set::add, Set::addAll);

        assertThat(actualIds).hasSize(GENERATED_IDS);
    }
}
