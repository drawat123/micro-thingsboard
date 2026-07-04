package com.divyam.telemetry.processor.config;

import org.springframework.context.annotation.Configuration;

/**
 * LESSON CODE — you write this.
 * <p>
 * The consumer-side mirror of the mqtt-transport module's {@code KafkaProducerConfig}. Where
 * the producer built a {@code ProducerFactory} + {@code KafkaTemplate}, the
 * consumer builds a {@code ConsumerFactory} + a listener container factory that
 * {@code @KafkaListener} methods run on.
 * <p>
 * The two beans to define (same shape as the producer config — a config map of
 * {@code ConsumerConfig.*} constants, then a factory built from it):
 *
 * <ol>
 *   <li><b>ConsumerFactory&lt;String, byte[]&gt;</b> — holds the connection
 *       config. Build a {@code Map<String, Object>} and put:
 *       <ul>
 *         <li>{@code ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG} — from properties</li>
 *         <li>{@code ConsumerConfig.GROUP_ID_CONFIG} — from properties</li>
 *         <li>{@code ConsumerConfig.AUTO_OFFSET_RESET_CONFIG} — from properties</li>
 *         <li>{@code ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG} — StringDeserializer
 *             (the key is the agentId, a String — the mirror of the producer's
 *             StringSerializer)</li>
 *         <li>{@code ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG} — ByteArrayDeserializer
 *             (the value is raw Protobuf bytes — mirror of ByteArraySerializer;
 *             we decode them ourselves in the listener, not here)</li>
 *       </ul>
 *       Return {@code new DefaultKafkaConsumerFactory<>(configProps)}.</li>
 *
 *   <li><b>ConcurrentKafkaListenerContainerFactory&lt;String, byte[]&gt;</b> —
 *       the thing {@code @KafkaListener} binds to. Construct it, call
 *       {@code setConsumerFactory(...)} with the factory above, and return it.
 *       (Later, in 3.3.4, this is also where the {@code DefaultErrorHandler} /
 *       DLQ wiring will live.)</li>
 * </ol>
 *
 * Reminders from your own conventions:
 *   - constructor injection of {@link KafkaConsumerProperties}, final field
 *   - {@code ConsumerConfig.*} constants, never raw strings
 *   - {@code @Bean} methods
 */
@Configuration
public class KafkaConsumerConfig {

    // TODO: final KafkaConsumerProperties field + constructor injection

    // TODO: @Bean ConsumerFactory<String, byte[]> consumerFactory()

    // TODO: @Bean ConcurrentKafkaListenerContainerFactory<String, byte[]> kafkaListenerContainerFactory(...)
}
