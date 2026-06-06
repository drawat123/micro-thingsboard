package com.divyam.telemetry.backend.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Type-safe binding for {@code server.tcp.*} properties.
 * <p>
 * Spring Boot reads matching keys from application.yml (or any active config
 * source) and constructs an instance of this record at startup. Invalid values
 * fail fast.
 */
@ConfigurationProperties(prefix = "server.tcp")
public record TcpServerProperties(int port) {
}