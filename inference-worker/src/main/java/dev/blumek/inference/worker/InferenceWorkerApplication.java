package dev.blumek.inference.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class InferenceWorkerApplication {

    public static void main(final String[] args) {
        SpringApplication.run(InferenceWorkerApplication.class, args);
    }
}
