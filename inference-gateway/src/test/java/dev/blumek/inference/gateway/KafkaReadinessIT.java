package dev.blumek.inference.gateway;

import dev.blumek.inference.messaging.InferenceTopics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.actuate.endpoint.CompositeHealthDescriptor;
import org.springframework.boot.health.actuate.endpoint.HealthEndpoint;
import org.springframework.boot.health.actuate.endpoint.IndicatedHealthDescriptor;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.client.RestTestClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(KafkaReadinessIT.Containers.class)
class KafkaReadinessIT {
    private static final int PARTITIONS = 1;
    private static final short REPLICAS = 1;

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

    @Autowired
    private KafkaContainer kafka;

    @LocalServerPort
    private int port;

    @BeforeEach
    void givenRequiredTopicsExist() throws Exception {
        try (final var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            final var topics = Stream
                    .of(InferenceTopics.JOBS, InferenceTopics.JOBS_RETRY, InferenceTopics.RESULTS, InferenceTopics.DLQ)
                    .map(name -> new NewTopic(name, PARTITIONS, REPLICAS))
                    .toList();
            admin.createTopics(topics).all().get();
        } catch (final ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    @Test
    void reportsReadinessUpWhenTheBrokerIsRunningAndTheTopicExists() {
        final var actualReadiness = whenReadinessIsProbed();

        assertThat(actualReadiness.getStatus()).isEqualTo(Status.UP);
        assertThat(actualReadiness.getComponents().get("kafka").getStatus()).isEqualTo(Status.UP);
    }

    private CompositeHealthDescriptor whenReadinessIsProbed() {
        return (CompositeHealthDescriptor) health.healthForPath("readiness");
    }

    @Test
    void answersTheReadinessProbeWithOkWhenTheBrokerIsRunning() {
        RestTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build()
                .get()
                .uri("/actuator/health/readiness")
                .exchange()
                .expectStatus()
                .isOk();
    }

    @Test
    void reportsTheClusterIdOfTheRunningBroker() {
        final var actualKafka = (IndicatedHealthDescriptor) whenReadinessIsProbed().getComponents().get("kafka");

        assertThat(actualKafka.getDetails()).containsKey("clusterId").containsEntry("nodes", 1);
    }
}
