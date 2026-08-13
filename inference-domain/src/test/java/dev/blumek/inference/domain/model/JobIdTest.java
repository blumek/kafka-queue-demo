package dev.blumek.inference.domain.model;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class JobIdTest {

    @Test
    void keepsGivenValue() {
        assertThat(new JobId("job-42").value()).isEqualTo("job-42");
    }

    @Test
    void rejectsNull() {
        assertThatNullPointerException().isThrownBy(() -> new JobId(null));
    }

    @Test
    void equalsByValue() {
        assertThat(new JobId("job-42")).isEqualTo(new JobId("job-42"));
        assertThat(new JobId("job-42")).isNotEqualTo(new JobId("job-43"));
    }

    @Test
    void newIdIsAUuid() {
        assertThatCode(() -> UUID.fromString(JobId.newId().value())).doesNotThrowAnyException();
    }

    @Test
    void newIdIsUniquePerCall() {
        Set<JobId> ids = Stream.generate(JobId::newId)
                .limit(1_000)
                .collect(HashSet::new, Set::add, Set::addAll);

        assertThat(ids).hasSize(1_000);
    }
}
