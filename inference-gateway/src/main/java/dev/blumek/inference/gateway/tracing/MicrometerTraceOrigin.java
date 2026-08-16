package dev.blumek.inference.gateway.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;

import java.util.Optional;

class MicrometerTraceOrigin implements TraceOrigin {
    private static final String VERSION = "00";
    private static final String SAMPLED = "01";
    private static final String NOT_SAMPLED = "00";
    private static final String FIELD_SEPARATOR = "-";

    private final Tracer tracer;

    MicrometerTraceOrigin(final Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public Optional<String> traceparent() {
        return Optional.ofNullable(tracer.currentSpan())
                .map(Span::context)
                .filter(MicrometerTraceOrigin::identifiesASpan)
                .map(MicrometerTraceOrigin::traceparentOf);
    }

    private static boolean identifiesASpan(final TraceContext context) {
        return !context.traceId().isBlank() && !context.spanId().isBlank();
    }

    private static String traceparentOf(final TraceContext context) {
        return String.join(FIELD_SEPARATOR, VERSION, context.traceId(), context.spanId(), samplingFlagsOf(context));
    }

    private static String samplingFlagsOf(final TraceContext context) {
        return Boolean.TRUE.equals(context.sampled()) ? SAMPLED : NOT_SAMPLED;
    }
}
