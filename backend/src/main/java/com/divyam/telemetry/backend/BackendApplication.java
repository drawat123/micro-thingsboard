package com.divyam.telemetry.backend;

import com.divyam.telemetry.backend.config.KafkaProperties;
import com.divyam.telemetry.backend.config.MqttBrokerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({MqttBrokerProperties.class, KafkaProperties.class})
public class BackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }
}