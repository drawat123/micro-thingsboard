package com.divyam.telemetry.metrics;

import com.divyam.telemetry.common.domain.SensorReading;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.time.Instant;
import java.util.List;

/**
 * Collects live system metrics using the OSHI library.
 * Designed to be called repeatedly on a schedule — each call returns a fresh snapshot of CPU and memory readings.
 */
public class SystemMetricsCollector {

    private static final String AGENT_ID = "mac-edge-01";

    private final CentralProcessor centralProcessor;

    private final GlobalMemory globalMemory;

    private long[] prevTicks;

    public SystemMetricsCollector() {
        SystemInfo si = new SystemInfo();

        HardwareAbstractionLayer hardwareAbstractionLayer = si.getHardware();
        centralProcessor = hardwareAbstractionLayer.getProcessor();
        globalMemory = hardwareAbstractionLayer.getMemory();
        prevTicks = centralProcessor.getSystemCpuLoadTicks();
    }

    /**
     * Takes a single snapshot of current system CPU and memory metrics.
     * <p>
     * <strong>⚠️ Note:</strong> The CPU value reflects average load since the previous call (or since construction, for the first call).
     * Calling this method back-to-back with no delay will produce noisy or near-zero readings.
     * Space calls at least ~500ms apart for meaningful values.
     * </p>
     * <p>
     * This method is thread-safe; concurrent callers will be serialized via intrinsic locking.
     * </p>
     * <p>
     * <strong>Recommendation:</strong> Rely on an external scheduler to drive this
     * method at regular, spaced intervals (e.g., every 1000ms) to ensure steady telemetry.
     * </p>
     *
     * @return A list of current hardware sensor readings.
     */
    public synchronized List<SensorReading> collect() {
        Instant now = Instant.now();

        // First iteration's load is cumulative since boot; subsequent iterations reflect the interval
        double load = centralProcessor.getSystemCpuLoadBetweenTicks(prevTicks);
        prevTicks = centralProcessor.getSystemCpuLoadTicks();
        double cpuLoadPercent = load * 100;

        long totalMemBytes = globalMemory.getTotal();
        long availBytes = globalMemory.getAvailable();
        long usedMemBytes = totalMemBytes - availBytes;
        double memUsedPercent = ((double) usedMemBytes / totalMemBytes) * 100;

        return List.of(
                new SensorReading(AGENT_ID + ".cpu.load.percent", cpuLoadPercent, now),
                new SensorReading(AGENT_ID + ".mem.used.bytes", usedMemBytes, now),
                new SensorReading(AGENT_ID + ".mem.used.percent", memUsedPercent, now),
                new SensorReading(AGENT_ID + ".mem.total.bytes", totalMemBytes, now)
        );
    }
}