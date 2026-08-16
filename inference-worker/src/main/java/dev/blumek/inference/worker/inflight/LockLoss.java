package dev.blumek.inference.worker.inflight;

import static java.util.Locale.ROOT;

public enum LockLoss {
    ACKNOWLEDGEMENT_REFUSED,
    ACKNOWLEDGEMENT_LOST,
    RENEWALS_EXHAUSTED;

    public String tag() {
        return name().toLowerCase(ROOT);
    }
}
