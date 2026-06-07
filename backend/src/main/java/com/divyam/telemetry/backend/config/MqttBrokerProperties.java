package com.divyam.telemetry.backend.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Type-safe binding for {@code mqtt.broker.*} properties.
 * <p>
 * This record holds configuration for the MQTT broker connection and subscription:
 * <ul>
 *   <li>{@code mqtt.broker.url}: The connection URL (e.g., tcp://localhost:1883)</li>
 *   <li>{@code mqtt.broker.clientId}: Unique identifier for this backend instance</li>
 *   <li>{@code mqtt.broker.topic}: The topic filter to subscribe to (e.g., telemetry/#)</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "mqtt.broker")
public record MqttBrokerProperties(String url, String clientId, String topic) {
}