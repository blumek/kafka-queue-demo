package dev.blumek.inference.gateway;

import dev.blumek.inference.messaging.InferenceTopics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.actuate.endpoint.CompositeHealthDescriptor;
import org.springframework.boot.health.actuate.endpoint.HealthEndpoint;
import org.springframework.boot.health.actuate.endpoint.IndicatedHealthDescriptor;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(MissingTopicsReadinessIT.Containers.class)
class MissingTopicsReadinessIT {
    @TestConfiguration(proxyBeanMethods = false)
    static class Containers {
        @Bean
        @ServiceConnection
        KafkaContainer kafkaContainer() {
            return new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.0"))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");
        }
    }

    @Autowired
    private HealthEndpoint health;

    @Test
    void reportsReadinessDownWhenTheRequiredTopicIsMissing() {
        final var actualReadiness = whenReadinessIsProbed();

        assertThat(actualReadiness.getStatus()).isEqualTo(Status.DOWN);
        assertThat(actualReadiness.getComponents().get("readinessState").getStatus()).isEqualTo(Status.UP);
    }

    private CompositeHealthDescriptor whenReadinessIsProbed() {
        return (CompositeHealthDescriptor) health.healthForPath("readiness");
    }

    @Test
    void namesTheMissingTopicInTheKafkaComponentDetails() {
        final var actualKafka = (IndicatedHealthDescriptor) whenReadinessIsProbed().getComponents().get("kafka");

        assertThat(actualKafka.getDetails())
                .containsKey("clusterId")
                .containsEntry("missingTopics", List.of(InferenceTopics.JOBS));
    }
}
