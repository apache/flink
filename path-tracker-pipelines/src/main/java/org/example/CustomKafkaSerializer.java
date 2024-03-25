package org.example;


import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.testcontainers.shaded.org.bouncycastle.util.Strings;

public class CustomKafkaSerializer implements
        KafkaRecordSerializationSchema<DataRecord> {

    private static final long serialVersionUID = 1L;

    private String topic;


    public CustomKafkaSerializer() {}

    public CustomKafkaSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            DataRecord element, KafkaSinkContext context, Long timestamp) {

        try {
            return new ProducerRecord<>(
                    topic,
                    Strings.toByteArray(Integer.toString(element.getValue())),
                    Strings.toByteArray(Integer.toString(element.getValue()))
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Could not serialize record: " + element, e);
        }
    }
}
