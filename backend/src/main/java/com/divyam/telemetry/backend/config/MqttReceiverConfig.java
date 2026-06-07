package com.divyam.telemetry.backend.config;

import com.divyam.telemetry.backend.service.TelemetryIngestionService;
import com.divyam.telemetry.common.domain.SensorReading;
import com.divyam.telemetry.common.net.SensorReadingCodec;
import com.divyam.telemetry.proto.SensorReadingProto;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.mqtt.inbound.Mqttv5PahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.MqttHeaders;

import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.integration.channel.DirectChannel;

/**
 * Wires the MQTT transport, Protobuf deserialization, and domain conversion
 * together into a Spring Integration flow.
 * <p>
 * This configuration owns the "ingestion pipeline" only: connecting to the broker,
 * subscribing to telemetry topics, and handling transformation errors.
 * Business logic lives in {@link TelemetryIngestionService}, which the flow
 * delegates to once a message is successfully parsed.
 */
@Configuration
public class MqttReceiverConfig {

    private static final Logger log = LoggerFactory.getLogger(MqttReceiverConfig.class);

    private final MqttBrokerProperties mqttProperties;

    private final TelemetryIngestionService ingestionService;

    public MqttReceiverConfig(MqttBrokerProperties mqttProperties, TelemetryIngestionService ingestionService) {
        this.mqttProperties = mqttProperties;
        this.ingestionService = ingestionService;
    }

    @Bean
    public MessageChannel mqttErrorChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow mqttErrorFlow() {
        return IntegrationFlow.from("mqttErrorChannel")
                .handle(message -> {
                    if (message instanceof ErrorMessage em) {
                        Throwable cause = em.getPayload();
                        log.error("MQTT Inbound Flow failure: {}", cause.getCause().getMessage());
                        // In a real system, we might increment a counter or send to a dead-letter queue here
                    } else {
                        log.error("Unknown error on mqttErrorChannel: {}", message.getPayload());
                    }
                })
                .get();
    }

    @Bean
    public MqttConnectionOptions mqttConnectionOptions() {
        MqttConnectionOptions options = new MqttConnectionOptions();
        options.setServerURIs(new String[]{mqttProperties.url()});
        options.setAutomaticReconnect(true);     // <-- Paho's supervisor
        options.setCleanStart(true);
        options.setKeepAliveInterval(30);        // ping every 30s to detect dead broker
        options.setConnectionTimeout(10);        // give up initial connect after 10s

        return options;
    }

    @Bean
    public Mqttv5PahoMessageDrivenChannelAdapter inboundMqttAdapter(MqttConnectionOptions options) {
        Mqttv5PahoMessageDrivenChannelAdapter adapter =
                new Mqttv5PahoMessageDrivenChannelAdapter(options, mqttProperties.clientId(), mqttProperties.topic());

        // Request the full Paho wrapper instead of raw byte[]
        adapter.setPayloadType(MqttMessage.class);

        // Explicitly route errors to our custom handler
        adapter.setErrorChannelName("mqttErrorChannel");

        return adapter;
    }

    @Bean
    public IntegrationFlow mqttInboundFlow(Mqttv5PahoMessageDrivenChannelAdapter mqttInboundAdapter) {
        return IntegrationFlow.from(mqttInboundAdapter)
                .enrichHeaders(spec -> spec.headerExpression("mqtt_qos", "payload.qos"))
                .transform(MqttMessage::getPayload)   // now just unwrap to byte[]
                .transform(this::parseProto)
                .transform(SensorReadingCodec::fromProto)
                .handle((GenericHandler<SensorReading>) (reading, headers) -> {
                    int qos = (int) headers.get("mqtt_qos");
                    String topic = (String) headers.get(MqttHeaders.RECEIVED_TOPIC);
                    log.info("topic={}, qos={}", topic, qos);
                    ingestionService.ingest(reading);
                    return null;
                })
                .get();
    }

    private SensorReadingProto parseProto(byte[] bytes) {
        try {
            return SensorReadingProto.parseFrom(bytes);
        } catch (Exception e) {
            // Rethrow and let Mqttv5PahoMessageDrivenChannelAdapter send it to mqttErrorChannel
            throw new RuntimeException("Protobuf deserialization failed", e);
        }
    }
}
