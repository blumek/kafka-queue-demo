package dev.blumek.inference.gateway.kafka;

import dev.blumek.inference.domain.model.InferenceRequest;
import dev.blumek.inference.gateway.submission.JobPublisher;
import dev.blumek.inference.gateway.tracing.TraceOrigin;
import dev.blumek.inference.messaging.InferenceSerdes;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaConnectionDetails;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
class KafkaProducerConfiguration {
    private static final Duration DELIVERY_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration LINGER = Duration.ofMillis(5);
    private static final int MAX_IN_FLIGHT = 5;
    private static final String COMPRESSION = "lz4";
    private static final String ACKS = "all";
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration MAX_BLOCK = Duration.ofSeconds(5);
    private static final Duration RETRY_AFTER = Duration.ofSeconds(5);
    private static final Duration PUBLISH_TIMEOUT = DELIVERY_TIMEOUT.plus(MAX_BLOCK).plusSeconds(5);
    private static final String CLIENT_ID = "inference-gateway-publisher";
    private static final int SEND_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int SEND_QUEUE_CAPACITY = 64;
    private static final long SEND_THREAD_KEEP_ALIVE_MS = 0L;

    static Map<String, Object> producerConfiguration(final Map<String, Object> base, final List<String> bootstrapServers) {
        final var configuration = new HashMap<>(base);
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configuration.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        configuration.put(ProducerConfig.ACKS_CONFIG, ACKS);
        configuration.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        configuration.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT);
        configuration.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Math.toIntExact(DELIVERY_TIMEOUT.toMillis()));
        configuration.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(REQUEST_TIMEOUT.toMillis()));
        configuration.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION);
        configuration.put(ProducerConfig.LINGER_MS_CONFIG, Math.toIntExact(LINGER.toMillis()));
        configuration.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK.toMillis());
        return configuration;
    }

    @Bean
    ProducerFactory<String, InferenceRequest> jobProducerFactory(final KafkaProperties properties,
                                                                 final KafkaConnectionDetails connectionDetails) {
        final var configuration = producerConfiguration(properties.buildProducerProperties(),
                connectionDetails.getProducer().getBootstrapServers());
        return new DefaultKafkaProducerFactory<>(configuration, new StringSerializer(), InferenceSerdes.requestSerializer());
    }

    @Bean
    KafkaTemplate<String, InferenceRequest> jobKafkaTemplate(final ProducerFactory<String, InferenceRequest> jobProducerFactory) {
        return new KafkaTemplate<>(jobProducerFactory);
    }

    @Bean
    ExecutorService jobSendExecutor() {
        return new ThreadPoolExecutor(SEND_THREADS, SEND_THREADS, SEND_THREAD_KEEP_ALIVE_MS, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(SEND_QUEUE_CAPACITY),
                Thread.ofPlatform().name("job-send-", 0).factory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    @Bean
    JobPublisher jobPublisher(final KafkaTemplate<String, InferenceRequest> jobKafkaTemplate,
                              final TraceOrigin traceOrigin,
                              final ExecutorService jobSendExecutor) {
        return new KafkaJobPublisher(jobKafkaTemplate, traceOrigin, jobSendExecutor, PUBLISH_TIMEOUT, RETRY_AFTER);
    }
}
