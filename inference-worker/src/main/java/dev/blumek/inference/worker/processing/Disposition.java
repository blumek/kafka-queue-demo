package dev.blumek.inference.worker.processing;

import java.time.Duration;

public sealed interface Disposition {
    record Accept()                           implements Disposition {}
    record Release(String reason)             implements Disposition {}
    record RejectAfterDlq(String reason)      implements Disposition {}
    record RetryViaDelayTopic(Duration delay) implements Disposition {}
}
