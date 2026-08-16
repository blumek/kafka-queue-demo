package dev.blumek.inference.worker.inflight;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.domain.model.JobId;
import dev.blumek.inference.domain.model.ModelId;
import dev.blumek.inference.messaging.InferenceTopics;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class RenewalScheduleTest {
    private static final Instant ACQUIRED_AT = Instant.parse("2026-08-16T09:00:00Z");
    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration HALF_THE_LOCK = LOCK_TIMEOUT.dividedBy(2);
    private static final Duration ONE_SECOND = Duration.ofSeconds(1);
    private static final int MAX_RENEWALS = 3;
    private static final int FIRST_DELIVERY = 1;

    private final RenewalSchedule schedule = new RenewalSchedule(HALF_THE_LOCK, MAX_RENEWALS);

    @Test
    void renewsHalfWayThroughTheLockOfAFreshlyAcquiredRecord() {
        assertThat(schedule.dueAt(givenAcquired())).isEqualTo(ACQUIRED_AT.plus(HALF_THE_LOCK));
    }

    @Test
    void measuresTheNextRenewalFromTheRefreshRatherThanTheAcquisition() {
        final var renewed = givenRenewed(1);

        assertThat(schedule.dueAt(renewed)).isEqualTo(ACQUIRED_AT.plus(HALF_THE_LOCK).plus(HALF_THE_LOCK));
    }

    @Test
    void keepsRenewingWhileTheJobIsWithinItsProcessingBudget() {
        assertThat(schedule.isExhausted(givenRenewed(MAX_RENEWALS - 1))).isFalse();
    }

    @Test
    void stopsRenewingOnceTheJobHasOutrunItsProcessingBudget() {
        assertThat(schedule.isExhausted(givenRenewed(MAX_RENEWALS))).isTrue();
    }

    @Test
    void stopsRenewingImmediatelyWhenNoRenewalIsBudgetedFor() {
        final var withoutRenewals = new RenewalSchedule(HALF_THE_LOCK, 0);

        assertThat(withoutRenewals.isExhausted(givenAcquired())).isTrue();
    }

    @Test
    void leavesTheRestOfTheLockAsMarginForALatePoll() {
        final var margin = Duration.between(schedule.dueAt(givenAcquired()), ACQUIRED_AT.plus(LOCK_TIMEOUT));

        assertThat(margin).isGreaterThanOrEqualTo(ONE_SECOND);
    }

    private static InFlight givenAcquired() {
        return InFlight.acquired(givenRef(), givenRequest(), FIRST_DELIVERY, ACQUIRED_AT);
    }

    private static InFlight givenRenewed(final int renewals) {
        return IntStream.range(0, renewals)
                .boxed()
                .reduce(givenAcquired(),
                        (inFlight, renewal) -> inFlight.refreshed(inFlight.lockRefreshedAt().plus(HALF_THE_LOCK)),
                        (one, another) -> another);
    }

    private static RecordRef givenRef() {
        return new RecordRef(new TopicPartition(InferenceTopics.JOBS, 0), 0L);
    }

    private static InferenceRequest givenRequest() {
        return new InferenceRequest(JobId.newId(), new ModelId("llama3:8b"), "why is the sky blue?", 128, ACQUIRED_AT);
    }
}
