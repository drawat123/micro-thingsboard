package com.divyam.telemetry;

import com.divyam.telemetry.agent.TelemetryAgent;
import com.divyam.telemetry.config.AgentConfig;
import com.divyam.telemetry.metrics.SystemMetricsCollector;
import com.divyam.telemetry.net.MqttTelemetryPublisher;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EdgeAgent {

    private static final Logger log = LoggerFactory.getLogger(EdgeAgent.class);

    public static void main(String[] args) {
        AgentConfig config = AgentConfig.load();

        SystemMetricsCollector collector = new SystemMetricsCollector(config.agentId());

        String clientId = "edge-agent-" + config.agentId();
        MqttTelemetryPublisher publisher = new MqttTelemetryPublisher(
                config.mqttBrokerUrl(),
                clientId,
                config.agentId());

        TelemetryAgent agent = new TelemetryAgent(collector, publisher, config.samplingPeriodSeconds());

        try {
            publisher.start();
        } catch (MqttException e) {
            log.error("Cannot connect to MQTT broker at {}", config.mqttBrokerUrl(), e);
            System.exit(1);
        }

        agent.start();

        // The JVM runs shutdown hooks only in case of normal terminations.
        // So, when an external force kills the JVM process abruptly, the JVM won’t get
        // a chance to execute shutdown hooks.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            agent.stop();
            publisher.stop();
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Main thread interrupted; exiting.");
        }
    }
}