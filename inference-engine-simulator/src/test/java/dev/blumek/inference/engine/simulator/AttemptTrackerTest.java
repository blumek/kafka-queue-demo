package dev.blumek.inference.engine.simulator;

import dev.blumek.inference.domain.model.JobId;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class AttemptTrackerTest {
    private static final int CONCURRENT_ATTEMPTS = 200;
    private static final int THREADS = 8;
    private static final int TRACKING_CEILING = 100_000;

    private final AttemptTracker tracker = new AttemptTracker();

    @Test
    void startsCountingAtOne() {
        final var givenJobId = givenJobId();

        final var actualAttempt = tracker.next(givenJobId);

        assertThat(actualAttempt).isEqualTo(1);
    }

    private JobId givenJobId() {
        return new JobId("job-a");
    }

    @Test
    void countsUpPerJob() {
        final var givenJobId = givenJobId();

        final var actualAttempts = IntStream.range(0, 5).map(attempt -> tracker.next(givenJobId)).boxed().toList();

        assertThat(actualAttempts).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void countsJobsIndependently() {
        final var givenJobId = givenJobId();
        final var givenOtherJobId = givenOtherJobId();
        tracker.next(givenJobId);
        tracker.next(givenJobId);

        final var actualAttempt = tracker.next(givenOtherJobId);

        assertThat(actualAttempt).isEqualTo(1);
    }

    private JobId givenOtherJobId() {
        return new JobId("job-b");
    }

    @Test
    void countsEveryConcurrentAttemptExactlyOnce() throws Exception {
        final var givenJobId = givenJobId();
        final var givenAttempts = givenConcurrentAttempts(givenJobId);

        final var actualAttempts = trackConcurrently(givenAttempts);

        assertThat(actualAttempts).map(Future::get).containsExactlyInAnyOrderElementsOf(expectedAttempts());
    }

    private List<Callable<Integer>> givenConcurrentAttempts(final JobId jobId) {
        return IntStream.range(0, CONCURRENT_ATTEMPTS)
                .<Callable<Integer>>mapToObj(attempt -> () -> tracker.next(jobId))
                .toList();
    }

    private static List<Future<Integer>> trackConcurrently(final List<Callable<Integer>> attempts) throws Exception {
        try (final var pool = Executors.newFixedThreadPool(THREADS)) {
            return pool.invokeAll(attempts);
        }
    }

    private static List<Integer> expectedAttempts() {
        return IntStream.rangeClosed(1, CONCURRENT_ATTEMPTS).boxed().toList();
    }

    @Test
    void forgetsOldJobsOnceTheTrackingCeilingIsReached() {
        final var givenJobId = givenJobId();
        tracker.next(givenJobId);
        givenTrackingCeilingReached();

        final var actualAttempt = tracker.next(givenJobId);

        assertThat(actualAttempt).isEqualTo(1);
    }

    private void givenTrackingCeilingReached() {
        IntStream.range(0, TRACKING_CEILING).forEach(job -> tracker.next(new JobId("job-" + job)));
    }
}
