package com.divyam.telemetry.backend;

import com.divyam.telemetry.common.domain.SensorReading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Handles every incoming SensorReading after it has been parsed and converted
 * to a domain object. This is the boundary where wire-level concerns end and
 * business logic begins.
 * <p>
 * Today: logs the reading.
 * Tomorrow: validation, routing, persistence, caching — all added here without
 * touching the TCP or Protobuf code.
 */
@Service
public class TelemetryIngestionService {

    private static final Logger log = LoggerFactory.getLogger(TelemetryIngestionService.class);

    public void ingest(SensorReading reading) {
        log.info("Ingested: {}", reading);
    }
}