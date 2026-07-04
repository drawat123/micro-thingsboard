package com.divyam.telemetry.processor;

import com.divyam.telemetry.processor.config.KafkaConsumerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point for the processor service — a Spring Boot Kafka consumer that
 * reads Protobuf-encoded telemetry off the {@code raw-telemetry} topic.
 * <p>
 * Note: this service has no web server (it depends on {@code spring-boot-starter},
 * not {@code -web}). It stays alive because the Kafka listener container is a
 * running Spring bean, not because an HTTP port is held open.
 */
@SpringBootApplication
@EnableConfigurationProperties(KafkaConsumerProperties.class)
public class TelemetryProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TelemetryProcessorApplication.class, args);
    }
}
