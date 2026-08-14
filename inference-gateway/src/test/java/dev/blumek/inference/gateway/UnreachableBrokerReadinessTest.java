package dev.blumek.inference.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.actuate.endpoint.CompositeHealthDescriptor;
import org.springframework.boot.health.actuate.endpoint.HealthEndpoint;
import org.springframework.boot.health.actuate.endpoint.IndicatedHealthDescriptor;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.test.web.servlet.client.RestTestClient;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
                properties = "spring.kafka.bootstrap-servers=localhost:1")
class UnreachableBrokerReadinessTest {
    @Autowired
    private HealthEndpoint health;

    @Autowired
    private ApplicationContext context;

    @LocalServerPort
    private int port;

    @Test
    void startsTheApplicationEvenThoughTheBrokerIsUnreachable() {
        assertThat(context.containsBean("kafkaHealthIndicator")).isTrue();
    }

    @Test
    void reportsReadinessDownWhenTheBrokerIsUnreachable() {
        final var actualReadiness = whenReadinessIsProbed();

        assertThat(actualReadiness.getStatus()).isEqualTo(Status.DOWN);
        assertThat(actualReadiness.getComponents().get("kafka").getStatus()).isEqualTo(Status.DOWN);
        assertThat(actualReadiness.getComponents().get("readinessState").getStatus()).isEqualTo(Status.UP);
    }

    private CompositeHealthDescriptor whenReadinessIsProbed() {
        return (CompositeHealthDescriptor) health.healthForPath("readiness");
    }

    @Test
    void answersTheReadinessProbeWithServiceUnavailableWhenTheBrokerIsUnreachable() {
        RestTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build()
                .get()
                .uri("/actuator/health/readiness")
                .exchange()
                .expectStatus()
                .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }

    @Test
    void reportsTheBrokerTimeoutAsTheKafkaComponentError() {
        final var actualKafka = (IndicatedHealthDescriptor) whenReadinessIsProbed().getComponents().get("kafka");

        assertThat(actualKafka.getDetails().get("error").toString())
                .startsWith("org.apache.kafka.common.errors.TimeoutException");
    }
}
