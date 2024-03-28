package org.example;


import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.testcontainers.shaded.org.bouncycastle.util.Strings;

public class CustomKafkaSerializer implements
        KafkaRecordSerializationSchema<DecorateRecord<Integer>> {

    private static final long serialVersionUID = 1L;

    private String topic;


    public CustomKafkaSerializer() {}

    public CustomKafkaSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            DecorateRecord<Integer> element, KafkaSinkContext context, Long timestamp) {

        try {
            return new ProducerRecord<>(
                    topic,
                    Strings.toByteArray(element.getPathInfo()),
                    Strings.toByteArray(String.format("%d-%d", element.getSeqNum(), element.getValue()))
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Could not serialize record: " + element, e);
        }
    }
}
