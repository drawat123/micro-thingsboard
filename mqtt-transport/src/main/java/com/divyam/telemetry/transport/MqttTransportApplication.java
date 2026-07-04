package com.divyam.telemetry.transport;

import com.divyam.telemetry.transport.config.KafkaProperties;
import com.divyam.telemetry.transport.config.MqttBrokerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({MqttBrokerProperties.class, KafkaProperties.class})
public class MqttTransportApplication {

    public static void main(String[] args) {
        SpringApplication.run(MqttTransportApplication.class, args);
    }
}