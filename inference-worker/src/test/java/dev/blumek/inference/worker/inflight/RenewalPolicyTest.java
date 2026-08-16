package dev.blumek.inference.worker.inflight;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RenewalPolicyTest {
    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration PROCESSING_BUDGET = Duration.ofMinutes(5);

    @Test
    void renewsHalfWayThroughTheLockByDefault() {
        final var actualSchedule = RenewalPolicy.over(PROCESSING_BUDGET).against(LOCK_TIMEOUT);

        assertThat(actualSchedule.renewAfter()).isEqualTo(Duration.ofSeconds(15));
    }

    @Test
    void renewsSoonerWhenAskedToLeaveAWiderMargin() {
        final var actualSchedule = new RenewalPolicy(PROCESSING_BUDGET, 0.25).against(LOCK_TIMEOUT);

        assertThat(actualSchedule.renewAfter()).isEqualTo(Duration.ofSeconds(7).plusMillis(500));
    }

    @Test
    void allowsAsManyRenewalsAsTheProcessingBudgetPaysFor() {
        final var actualSchedule = RenewalPolicy.over(PROCESSING_BUDGET).against(LOCK_TIMEOUT);

        assertThat(actualSchedule.maxRenewals()).isEqualTo(20);
    }

    @Test
    void followsTheLockTheGroupIsActuallyConfiguredWith() {
        final var actualSchedule = RenewalPolicy.over(Duration.ofSeconds(10)).against(Duration.ofSeconds(2));

        assertThat(actualSchedule).isEqualTo(new RenewalSchedule(Duration.ofSeconds(1), 10));
    }

    @Test
    void allowsNoRenewalAtAllToAJobBudgetedForLessThanOneInterval() {
        final var actualSchedule = RenewalPolicy.over(Duration.ofMillis(500)).against(Duration.ofSeconds(2));

        assertThat(actualSchedule.maxRenewals()).isZero();
    }

    @Test
    void refusesAFractionThatLeavesNoMarginForALatePoll() {
        assertThatThrownBy(() -> new RenewalPolicy(PROCESSING_BUDGET, 1.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("margin");
    }

    @Test
    void refusesAFractionThatIsNoFractionAtAll() {
        assertThatThrownBy(() -> new RenewalPolicy(PROCESSING_BUDGET, 0.0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refusesAJobWithoutAProcessingBudget() {
        assertThatThrownBy(() -> RenewalPolicy.over(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("processing budget");
    }
}
