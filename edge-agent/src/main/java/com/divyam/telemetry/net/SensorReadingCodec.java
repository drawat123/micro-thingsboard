package com.divyam.telemetry.net;

import com.divyam.telemetry.domain.SensorReading;
import com.divyam.telemetry.proto.SensorReadingProto;

import java.time.Instant;

/**
 * Bidirectional converter between the domain {@link SensorReading} record
 * and the generated Protobuf {@link SensorReadingProto}.
 * <p>
 * This is an anti-corruption layer: business code uses the immutable record,
 * the wire uses the Protobuf class, and this codec is the only place that
 * knows both worlds.
 * <p>
 * Pure and stateless — safe to call concurrently.
 */
public final class SensorReadingCodec {

    private SensorReadingCodec() {
        // utility class — no instances
    }

    /**
     * Convert a domain SensorReading to its on-wire Protobuf representation.
     *
     * @param reading the domain object (must not be null)
     * @return a fully-built Protobuf message ready for writeDelimitedTo()
     */
    public static SensorReadingProto toProto(SensorReading reading) {
        return SensorReadingProto.newBuilder()
                .setSensorId(reading.sensorId())
                .setValue(reading.value())
                .setTimestampMillis(reading.timestamp().toEpochMilli())
                .build();
    }

    /**
     * Convert an on-wire Protobuf message to a domain SensorReading.
     * <p>
     * Note: this is where wire-format validation happens. If the incoming
     * Protobuf has an empty sensor_id or invalid timestamp, the domain
     * SensorReading constructor will throw IllegalArgumentException —
     * exactly what we want (fail-fast at the boundary).
     *
     * @param proto the wire-format message (must not be null)
     * @return a new domain SensorReading
     * @throws IllegalArgumentException if the proto contains invalid data
     */
    public static SensorReading fromProto(SensorReadingProto proto) {
        return new SensorReading(
                proto.getSensorId(),
                proto.getValue(),
                Instant.ofEpochMilli(proto.getTimestampMillis())
        );
    }
}