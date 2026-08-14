package dev.blumek.inference.messaging;

public final class InferenceTopics {
    public static final String JOBS = "inference.jobs";
    public static final String JOBS_RETRY = "inference.jobs.retry";
    public static final String RESULTS = "inference.results";
    public static final String DLQ = "inference.dlq";

    private InferenceTopics() {}
}
