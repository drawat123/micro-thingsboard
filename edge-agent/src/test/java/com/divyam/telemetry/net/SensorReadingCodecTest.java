package com.divyam.telemetry.net;

import com.divyam.telemetry.domain.SensorReading;
import com.divyam.telemetry.proto.SensorReadingProto;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;

class SensorReadingCodecTest {

    @Test
    void testToProto() {
        Instant now = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        SensorReading reading = new SensorReading("temp-01", 25.5, now);

        SensorReadingProto proto = SensorReadingCodec.toProto(reading);

        assertEquals("temp-01", proto.getSensorId());
        assertEquals(25.5, proto.getValue());
        assertEquals(now.toEpochMilli(), proto.getTimestampMillis());
    }

    @Test
    void testFromProto() {
        long millis = System.currentTimeMillis();
        SensorReadingProto proto = SensorReadingProto.newBuilder()
                .setSensorId("humi-02")
                .setValue(60.0)
                .setTimestampMillis(millis)
                .build();

        SensorReading reading = SensorReadingCodec.fromProto(proto);

        assertEquals("humi-02", reading.sensorId());
        assertEquals(60.0, reading.value());
        assertEquals(Instant.ofEpochMilli(millis), reading.timestamp());
    }

    @Test
    void testBidirectionalConsistency() {
        SensorReading original = new SensorReading("press-03", 1013.25, Instant.now().truncatedTo(ChronoUnit.MILLIS));

        SensorReading restored = SensorReadingCodec.fromProto(SensorReadingCodec.toProto(original));

        assertEquals(original, restored);
    }

    @Test
    void testTimestampPrecisionLoss() {
        // Create an Instant with nanosecond precision
        Instant withNanos = Instant.parse("2026-05-21T10:30:00.123456789Z");
        SensorReading original = new SensorReading("temp-01", 25.0, withNanos);

        SensorReading restored = SensorReadingCodec.fromProto(SensorReadingCodec.toProto(original));

        // Assert that the restored reading has lost the nanoseconds (it only has millis)
        assertEquals(withNanos.toEpochMilli(), restored.timestamp().toEpochMilli());
        assertEquals(0, restored.timestamp().getNano() % 1_000_000);

        // Assert they are NOT equal if we don't truncate the original
        assertNotEquals(original, restored);
    }

    @Test
    void testValidationFailureOnFromProto() {
        SensorReadingProto invalidProto = SensorReadingProto.newBuilder()
                .setSensorId("") // blank sensorId is invalid
                .setValue(10.0)
                .setTimestampMillis(System.currentTimeMillis())
                .build();

        assertThrows(IllegalArgumentException.class, () -> SensorReadingCodec.fromProto(invalidProto));
    }
}