package com.divyam.telemetry;

import com.divyam.telemetry.agent.TelemetryAgent;
import com.divyam.telemetry.config.AgentConfig;
import com.divyam.telemetry.metrics.SystemMetricsCollector;
import com.divyam.telemetry.net.TelemetrySender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class EdgeAgent {

    private static final Logger log = LoggerFactory.getLogger(EdgeAgent.class);

    public static void main(String[] args) {
        AgentConfig config = AgentConfig.load();

        SystemMetricsCollector collector = new SystemMetricsCollector(config.agentId());

        TelemetrySender sender = new TelemetrySender(config.backendHost(), config.backendPort());

        TelemetryAgent agent = new TelemetryAgent(collector, sender, config.samplingPeriodSeconds());

        try {
            sender.start();
        } catch (IOException e) {
            log.error("Cannot start sender (is SmokeServer running on localhost:5555?)", e);
            System.exit(1);
        }
        agent.start();

        // The JVM runs shutdown hooks only in case of normal terminations.
        // So, when an external force kills the JVM process abruptly, the JVM won’t get a chance to execute shutdown hooks.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            agent.stop();
            sender.stop();
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Main thread interrupted; exiting.");
        }
    }
}