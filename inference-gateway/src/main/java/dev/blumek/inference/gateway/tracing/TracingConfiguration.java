package dev.blumek.inference.gateway.tracing;

import io.micrometer.tracing.Tracer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class TracingConfiguration {

    @Bean
    TraceOrigin traceOrigin(final Tracer tracer) {
        return new MicrometerTraceOrigin(tracer);
    }
}
