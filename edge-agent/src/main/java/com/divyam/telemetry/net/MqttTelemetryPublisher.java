package com.divyam.telemetry.net;

import com.divyam.telemetry.common.domain.SensorReading;
import com.divyam.telemetry.common.net.SensorReadingCodec;
import com.divyam.telemetry.proto.SensorReadingProto;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publishes SensorReadings to an MQTT broker.
 * <p>
 * Topic structure: {@code telemetry/{agentId}/{category}/{specificMetric}}
 * <p>
 * Each sensor's ID (e.g., "mac-edge-01.cpu.load.percent") is split into
 * the device ID, category, and specific metric segments for routing.
 * <p>
 * Connection lifecycle (reconnect, backoff, outbox-during-disconnect)
 * is handled by Paho — see {@link MqttConnectionOptions}.
 */
public class MqttTelemetryPublisher {

    private static final Logger log = LoggerFactory.getLogger(MqttTelemetryPublisher.class);

    private static final int QOS_TELEMETRY = 0;   // fire-and-forget; loss acceptable

    private final String brokerUrl;
    private final String clientId;
    private final String agentId;

    private MqttClient client;

    public MqttTelemetryPublisher(String brokerUrl, String clientId, String agentId) {
        this.brokerUrl = brokerUrl;
        this.clientId = clientId;
        this.agentId = agentId;
    }

    /**
     * Connect to the broker. Paho handles reconnection internally; if the
     * initial connect fails, we throw and let the caller decide.
     */
    public void start() throws MqttException {
        client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());

        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setAutomaticReconnect(true);     // <-- Paho's supervisor
        options.setCleanStart(true);
        options.setKeepAliveInterval(30);        // ping every 30s to detect dead broker
        options.setConnectionTimeout(10);        // give up initial connect after 10s

        log.info("Connecting to MQTT broker at {} as client '{}'", brokerUrl, clientId);
        client.connect(options);
        log.info("Connected. MQTT publisher ready.");
    }

    /**
     * Publish a reading. Non-blocking: Paho enqueues internally.
     * If disconnected, message is buffered until reconnect (subject to Paho's outbox config).
     */
    public void publish(SensorReading reading) {
        try {
            String topic = buildTopic(reading.sensorId());
            byte[] payload = SensorReadingCodec.toProto(reading).toByteArray();

            MqttMessage message = new MqttMessage(payload);
            message.setQos(QOS_TELEMETRY);

            client.publish(topic, message);
        } catch (MqttException e) {
            log.warn("Failed to publish reading for {}: {}", reading.sensorId(), e.getMessage());
        }
    }

    /**
     * Disconnect cleanly. Pending messages with QoS > 0 may be flushed first
     * depending on broker behavior.
     */
    public void stop() {
        if (client != null && client.isConnected()) {
            try {
                log.info("Disconnecting MQTT publisher...");
                client.disconnect();
                client.close();
                log.info("MQTT publisher disconnected.");
            } catch (MqttException e) {
                log.warn("Error during MQTT disconnect: {}", e.getMessage());
            }
        }
    }

    /**
     * Convert a sensorId like "mac-edge-01.cpu.load.percent" to a topic like
     * "telemetry/mac-edge-01/cpu/load.percent".
     * <p>
     * Format: {agentId}.{category}.{metricRest...}
     * Strategy: strip the agentId prefix, then split off the category as the next segment.
     */
    private String buildTopic(String sensorId) {
        // Strip the leading "{agentId}." prefix
        String suffix = sensorId.startsWith(agentId + ".")
                ? sensorId.substring(agentId.length() + 1)
                : sensorId;

        // Split off the category (e.g., "cpu", "mem") from the rest
        int firstDot = suffix.indexOf('.');
        if (firstDot < 0) {
            // No category; fall back to a flat structure
            return "telemetry/" + agentId + "/" + suffix;
        }

        String category = suffix.substring(0, firstDot);
        String specificMetric = suffix.substring(firstDot + 1);

        return "telemetry/" + agentId + "/" + category + "/" + specificMetric;
    }
}