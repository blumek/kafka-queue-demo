package dev.blumek.inference.gateway.submission;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
class SubmissionConfiguration {

    @Bean
    SubmitJobMapper submitJobMapper() {
        return new SubmitJobMapper(Clock.systemUTC());
    }

    @Bean
    JobSubmissionService jobSubmissionService(final JobPublisher jobPublisher, final SubmitJobMapper submitJobMapper) {
        return new JobSubmissionService(jobPublisher, submitJobMapper);
    }
}
