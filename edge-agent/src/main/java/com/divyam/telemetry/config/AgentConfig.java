package com.divyam.telemetry.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Plain-Java configuration loader for the Edge Agent.
 * <p>
 * Layered resolution (lowest to highest priority):
 *   1. Defaults baked into edge-agent.properties on the classpath
 *   2. Environment variables (AGENT_ID, BACKEND_HOST, BACKEND_PORT, SAMPLING_PERIOD_SECONDS)
 *   3. JVM system properties (-Dagent.id=..., etc.)
 */
public final class AgentConfig {

    private static final Logger log = LoggerFactory.getLogger(AgentConfig.class);
    private static final String CONFIG_FILE = "edge-agent.properties";

    private final String agentId;
    private final String backendHost;
    private final int backendPort;
    private final long samplingPeriodSeconds;

    private AgentConfig(String agentId, String backendHost, int backendPort, long samplingPeriodSeconds) {
        this.agentId = agentId;
        this.backendHost = backendHost;
        this.backendPort = backendPort;
        this.samplingPeriodSeconds = samplingPeriodSeconds;
    }

    public static AgentConfig load() {
        Properties props = new Properties();
        try (InputStream in = AgentConfig.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (in == null) {
                throw new IllegalStateException("Configuration file not found: " + CONFIG_FILE);
            }
            props.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load configuration", e);
        }

        AgentConfig config = new AgentConfig(
                resolve("agent.id", props),
                resolve("backend.host", props),
                Integer.parseInt(resolve("backend.port", props)),
                Long.parseLong(resolve("sampling.period.seconds", props))
        );

        log.info("Loaded configuration: agentId={}, backend={}:{}, samplingPeriod={}s",
                config.agentId, config.backendHost, config.backendPort, config.samplingPeriodSeconds);

        return config;
    }

    /**
     * Resolve a property by checking JVM system properties first,
     * then environment variables (key converted to UPPER_SNAKE_CASE),
     * then the loaded properties file.
     */
    private static String resolve(String key, Properties fileProps) {
        String fromSystem = System.getProperty(key);
        if (fromSystem != null) return fromSystem;

        String envKey = key.toUpperCase().replace('.', '_');
        String fromEnv = System.getenv(envKey);
        if (fromEnv != null) return fromEnv;

        String fromFile = fileProps.getProperty(key);
        if (fromFile == null) {
            throw new IllegalStateException("Required configuration missing: " + key);
        }
        return fromFile;
    }

    public String agentId() { return agentId; }
    public String backendHost() { return backendHost; }
    public int backendPort() { return backendPort; }
    public long samplingPeriodSeconds() { return samplingPeriodSeconds; }
}