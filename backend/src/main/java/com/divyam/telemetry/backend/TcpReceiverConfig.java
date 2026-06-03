package com.divyam.telemetry.backend;

import com.divyam.telemetry.common.domain.SensorReading;
import com.divyam.telemetry.common.net.SensorReadingCodec;
import com.divyam.telemetry.proto.SensorReadingProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.ip.dsl.Tcp;
import org.springframework.integration.ip.tcp.connection.AbstractServerConnectionFactory;
import org.springframework.integration.ip.tcp.serializer.AbstractByteArraySerializer;

/**
 * Wires the TCP transport, Protobuf framing, and domain conversion together
 * into a Spring Integration flow.
 * <p>
 * This file owns NETWORK PLUMBING only. Business logic lives in
 * {@link TelemetryIngestionService}, which the flow delegates to.
 */
@Configuration
public class TcpReceiverConfig {

    private static final Logger log = LoggerFactory.getLogger(TcpReceiverConfig.class);

    private static final int PORT = 5555;

    private final TelemetryIngestionService ingestionService;

    public TcpReceiverConfig(TelemetryIngestionService ingestionService) {
        this.ingestionService = ingestionService;
    }

    @Bean
    public AbstractByteArraySerializer protobufVarintSerializer() {
        return new ProtobufVarintLengthSerializer();
    }

    @Bean
    public AbstractServerConnectionFactory tcpServerConnectionFactory(
            AbstractByteArraySerializer serializer) {
        return Tcp.netServer(PORT)
                .deserializer(serializer)
                .getObject();
    }

    @Bean
    public IntegrationFlow inboundTelemetryFlow(AbstractServerConnectionFactory cf) {
        return IntegrationFlow.from(Tcp.inboundAdapter(cf))
                .transform(this::parseProto)
                .transform(SensorReadingCodec::fromProto)
                .handle(message -> ingestionService.ingest((SensorReading) message.getPayload()))
                .get();
    }

    private SensorReadingProto parseProto(byte[] bytes) {
        try {
            return SensorReadingProto.parseFrom(bytes);
        } catch (Exception e) {
            log.error("Failed to parse Protobuf payload", e);
            throw new RuntimeException(e);
        }
    }
}