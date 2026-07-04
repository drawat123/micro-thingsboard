package com.divyam.telemetry.transport.service;

import com.divyam.telemetry.transport.config.KafkaProperties;
import com.divyam.telemetry.common.domain.SensorReading;
import com.divyam.telemetry.common.net.SensorReadingCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Handles every incoming SensorReading after it has been parsed and converted
 * to a domain object. This is the boundary where wire-level concerns end and
 * business logic begins.
 * <p>
 * Today: logs the reading, publishes to kafka.
 * Tomorrow: validation, routing, persistence, caching — all added here without
 * touching the TCP or Protobuf code.
 */
@Service
public class TelemetryIngestionService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryIngestionService.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    private final KafkaProperties kafkaProperties;

    public TelemetryIngestionService(KafkaTemplate<String, byte[]> kafkaTemplate, KafkaProperties kafkaProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    public void ingest(SensorReading reading) {
        log.info("Ingested: {}", reading);

        String sensorId = reading.sensorId();

        int firstDot = sensorId.indexOf('.');
        if (firstDot < 0) {
            log.warn("Cannot extract agentId from sensorId: {}", sensorId);
            return;
        }
        String agentId = sensorId.substring(0, firstDot);

        byte[] value = SensorReadingCodec.toProto(reading).toByteArray();

        kafkaTemplate.send(kafkaProperties.topic(), agentId, value)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.warn("Failed to send to Kafka: {}", ex.getMessage());
                    } else {
                        // result.getRecordMetadata() has partition + offset
                    }
                });
    }
}