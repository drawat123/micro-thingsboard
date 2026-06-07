package com.divyam.telemetry.agent;

import com.divyam.telemetry.common.domain.SensorReading;
import com.divyam.telemetry.metrics.SystemMetricsCollector;
import com.divyam.telemetry.net.MqttTelemetryPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Samples system metrics on a fixed cadence and publishes each reading
 * to the MQTT broker for transmission to the backend.
 */
public class TelemetryAgent {

    private static final Logger log = LoggerFactory.getLogger(TelemetryAgent.class);

    private final SystemMetricsCollector collector;

    private final ScheduledExecutorService scheduler;

    private final MqttTelemetryPublisher publisher;

    private final long samplingPeriodSeconds;

    private final AtomicBoolean started = new AtomicBoolean(false);

    public TelemetryAgent(SystemMetricsCollector collector, MqttTelemetryPublisher publisher, long samplingPeriodSeconds) {
        this.collector = collector;
        this.publisher = publisher;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.samplingPeriodSeconds = samplingPeriodSeconds;
    }

    /**
     * Begin sampling. After this returns, the agent is "alive" — a background thread is now firing collect() on a schedule.
     * The agent schedules metrics collection with an initial delay of zero, ensuring immediate initialization visibility
     * The stateful CPU calculation relies on the time elapsed
     * since the constructor baseline or the previous cycle to report windowed intervals.
     * <p>
     * <strong>Warning:</strong> This agent is single-use. Once {@link #stop()} has been called,
     * this instance cannot be restarted. Create a new {@code TelemetryAgent} if continued
     * sampling is required.
     * </p>
     */
    public void start() {
        // compareAndSet(false, true) does both operations atomically at the CPU level — it's a single hardware instruction (CAS — Compare-And-Swap)
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Agent already started");
        }

        log.info("Starting Telemetry Agent (sampling every {}s)...", samplingPeriodSeconds);

        Runnable sampler = () -> {
            try {
                var readings = collector.collect();

                for (SensorReading reading : readings) {
                    publisher.publish(reading);    // <- was sender.enqueue(reading)
                }

                log.debug("Published {} readings", readings.size());
            } catch (Throwable t) {
                log.error("Unexpected error during metrics collection", t);
            }
        };

        scheduler.scheduleWithFixedDelay(sampler, 0, samplingPeriodSeconds, TimeUnit.SECONDS);
    }

    /**
     * Stop sampling cleanly. After this returns, no new samples will be taken,
     * and any in-flight sample has been given a chance to complete.
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        log.info("Shutting down Telemetry Agent...");

        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                log.warn("Agent tasks did not complete in time, forcing shutdown...");
                scheduler.shutdownNow();
            }
            log.info("Telemetry Agent stopped successfully.");
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
            scheduler.shutdownNow();
            // Restore the interrupted status flag so upstream callers know a shutdown is in progress
            Thread.currentThread().interrupt();
        }
    }
}