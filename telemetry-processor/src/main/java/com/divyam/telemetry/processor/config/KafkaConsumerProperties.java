package com.divyam.telemetry.processor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Grouped Kafka consumer configuration, bound from the {@code kafka:} block in
 * application.yml. Mirrors the mqtt-transport module's {@code KafkaProperties}
 * record on the producer side.
 *
 * @param bootstrapServers Kafka broker address(es), e.g. "localhost:9092"
 * @param topic            the topic to consume from ("raw-telemetry")
 * @param groupId          the consumer group id — consumers sharing this id
 *                         split the topic's partitions between them
 * @param autoOffsetReset  what to do when there's no committed offset for this
 *                         group: "earliest" (from the start of the log) or
 *                         "latest" (only messages produced after we join)
 */
@ConfigurationProperties(prefix = "kafka")
public record KafkaConsumerProperties(
        String bootstrapServers,
        String topic,
        String groupId,
        String autoOffsetReset
) {
}
