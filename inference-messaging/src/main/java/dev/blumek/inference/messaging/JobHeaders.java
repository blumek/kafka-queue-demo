package dev.blumek.inference.messaging;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.time.Instant;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public final class JobHeaders {
    public static final String NOT_BEFORE = "inference-not-before";
    public static final String TRACEPARENT = "traceparent";

    private JobHeaders() {}

    public static void putNotBefore(final Headers headers, final Instant notBefore) {
        put(headers, NOT_BEFORE, requireNonNull(notBefore).toString());
    }

    public static Optional<Instant> notBefore(final Headers headers) {
        return value(headers, NOT_BEFORE).map(Instant::parse);
    }

    public static void putTraceparent(final Headers headers, final String traceparent) {
        put(headers, TRACEPARENT, requireNonNull(traceparent));
    }

    public static Optional<String> traceparent(final Headers headers) {
        return value(headers, TRACEPARENT);
    }

    private static void put(final Headers headers, final String key, final String value) {
        requireNonNull(headers);
        headers.remove(key);
        headers.add(key, value.getBytes(UTF_8));
    }

    private static Optional<String> value(final Headers headers, final String key) {
        requireNonNull(headers);
        return Optional.ofNullable(headers.lastHeader(key))
                .map(Header::value)
                .map(bytes -> new String(bytes, UTF_8));
    }
}
