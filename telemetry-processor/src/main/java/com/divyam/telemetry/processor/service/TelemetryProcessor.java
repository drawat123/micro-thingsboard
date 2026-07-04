package com.divyam.telemetry.processor.service;

import org.springframework.stereotype.Service;

/**
 * LESSON CODE — you write this.
 * <p>
 * The consuming end of the pipeline. This is the mirror of the mqtt-transport
 * module's {@code TelemetryIngestionService}: that module encoded a {@link
 * com.divyam.telemetry.common.domain.SensorReading} to Protobuf bytes and
 * produced them to {@code raw-telemetry}; here we consume those bytes and decode
 * them back into the domain object.
 * <p>
 * Write one method annotated with {@code @KafkaListener}:
 *
 * <pre>
 *   &#64;KafkaListener(topics = "...", groupId = "...", containerFactory = "kafkaListenerContainerFactory")
 *   public void onMessage(byte[] value) { ... }
 * </pre>
 *
 * Points to reason about as you write it:
 * <ul>
 *   <li><b>Topic / groupId</b>: don't hard-code strings. Pull them from
 *       {@link com.divyam.telemetry.processor.config.KafkaConsumerProperties}
 *       via SpEL ({@code "#{@kafkaConsumerProperties.topic()}"}) so there's a
 *       single source of truth — same no-dead-config discipline as the producer.</li>
 *   <li><b>Decode at the boundary</b>: {@code SensorReadingProto.parseFrom(value)}
 *       then {@code SensorReadingCodec.fromProto(...)}. This is the 4th and final
 *       codec call in the pipeline. Guard it — a malformed record shouldn't kill
 *       the listener thread (think about what SHOULD happen: log + skip now;
 *       DLQ in 3.3.4).</li>
 *   <li><b>Method signature</b>: start with just {@code byte[] value}. Once that
 *       works, consider pulling the key / partition / offset via {@code @Header}
 *       (e.g. {@code KafkaHeaders.RECEIVED_KEY}, {@code RECEIVED_PARTITION},
 *       {@code OFFSET}) to log where each record came from — the consumer-side
 *       equivalent of the topic/qos logging you did in the MQTT flow.</li>
 *   <li><b>Offset commits</b>: with the default setup Spring auto-commits offsets
 *       after the listener returns normally. Returning normally = "I processed
 *       this." Throwing = "I didn't" (redelivery / error-handler territory).
 *       Keep it simple for now; we'll make commit semantics explicit in 3.3.4.</li>
 * </ul>
 */
@Service
public class TelemetryProcessor {

    // TODO: Logger

    // TODO: constructor-inject anything you need (nothing required yet)

    // TODO: @KafkaListener method — receive byte[], decode via codec, process (log for now)
}
