package dev.blumek.inference.gateway.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MicrometerTraceOriginTest {
    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String SPAN_ID = "00f067aa0ba902b7";
    private static final String NO_ID = "";

    private final Tracer tracer = mock(Tracer.class);

    private final TraceOrigin origin = new MicrometerTraceOrigin(tracer);

    @Test
    void spellsTheSpanInFlightAsAW3cTraceparent() {
        givenTheSpanInFlightIs(TRACE_ID, SPAN_ID, true);

        assertThat(origin.traceparent()).contains("00-" + TRACE_ID + "-" + SPAN_ID + "-01");
    }

    @Test
    void tellsWhoeverContinuesTheTraceThatItIsNotBeingRecorded() {
        givenTheSpanInFlightIs(TRACE_ID, SPAN_ID, false);

        assertThat(origin.traceparent()).contains("00-" + TRACE_ID + "-" + SPAN_ID + "-00");
    }

    @Test
    void treatsAnUndecidedSamplingDecisionAsNotRecorded() {
        givenTheSpanInFlightIs(TRACE_ID, SPAN_ID, null);

        assertThat(origin.traceparent()).contains("00-" + TRACE_ID + "-" + SPAN_ID + "-00");
    }

    @Test
    void offersNothingWhenNothingHasStartedATrace() {
        when(tracer.currentSpan()).thenReturn(null);

        assertThat(origin.traceparent()).isEmpty();
    }

    @Test
    void offersNothingWhenTheSpanInFlightIdentifiesNoTrace() {
        givenTheSpanInFlightIs(NO_ID, NO_ID, true);

        assertThat(origin.traceparent()).isEmpty();
    }

    private void givenTheSpanInFlightIs(final String traceId, final String spanId, final Boolean sampled) {
        final var span = mock(Span.class);
        when(span.context()).thenReturn(new GivenTraceContext(traceId, spanId, sampled));
        when(tracer.currentSpan()).thenReturn(span);
    }

    private record GivenTraceContext(String traceId, String spanId, Boolean sampled) implements TraceContext {
        @Override
        public String parentId() {
            return null;
        }
    }
}
