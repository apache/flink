package org.apache.flink.connectors.test.kafka.external;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;
import org.apache.flink.table.data.RowData;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Properties;

/** Writer. */
public class KafkaSourceSplitDataWriter implements SourceSplitDataWriter<String> {

    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final TopicPartition topicPartition;
    private final SerializationSchema<RowData> rowDataSerializationSchema;

    public KafkaSourceSplitDataWriter(
            Properties producerProperties,
            TopicPartition topicPartition,
            SerializationSchema<RowData> rowDataSerializationSchema) {
        this.kafkaProducer = new KafkaProducer<>(producerProperties);
        this.topicPartition = topicPartition;
        this.rowDataSerializationSchema = rowDataSerializationSchema;
    }

    @Override
    public void writeRecords(Collection<String> records) {
        for (String record : records) {
            ProducerRecord<byte[], byte[]> producerRecord =
                    new ProducerRecord<>(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            null,
                            record.getBytes(StandardCharsets.UTF_8));
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.flush();
    }

    @Override
    public void writeRowDataRecords(Collection<RowData> records) {
        for (RowData record : records) {
            ProducerRecord<byte[], byte[]> producerRecord =
                    new ProducerRecord<>(
                            topicPartition.topic(),
                            topicPartition.partition(),
                            null,
                            rowDataSerializationSchema.serialize(record));
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.flush();
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }
}
