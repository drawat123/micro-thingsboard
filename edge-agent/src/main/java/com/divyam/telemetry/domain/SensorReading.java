package com.divyam.telemetry.domain;

import java.time.Instant;

/**
 * Immutable snapshot of a single sensor measurement.
 * <p>
 * This is a value object — two readings with identical fields are
 * considered equal, regardless of object identity.
 */
public record SensorReading(
        String sensorId,
        double value,
        Instant timestamp
) {
    /**
     * Compact constructor — runs validation before the record is created.
     * If validation fails, the object is never constructed.
     */
    public SensorReading {
        if (sensorId == null || sensorId.isBlank()) {
            throw new IllegalArgumentException("sensorId must not be blank");
        }
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp must not be null");
        }
    }
}