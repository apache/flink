package org.example;


import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.bouncycastle.util.Strings;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class CustomKafkaSerializer implements
        KafkaRecordSerializationSchema<DecorateRecord<Integer>> {

    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private static final long serialVersionUID = 1L;

    private String topic;

    //todo: make this map a part of the sink state
    private HashMap<String, Integer> pathIdToPartitionId;




    public CustomKafkaSerializer(String topic)
    {
        this.topic = topic;
        this.pathIdToPartitionId = new HashMap<>();
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            DecorateRecord<Integer> element, KafkaSinkContext context, Long timestamp) {
        String pathId = element.getPathInfo();
        int partitionId = 0;
        if (!pathIdToPartitionId.containsKey(pathId)){

            int pId = pathIdToPartitionId.size();
            partitionId = pId;
            pathIdToPartitionId.put(pathId, pId);
            logger.info(String.format("Encountered new path ID %s, current total unique path count: %d", pathId, pathIdToPartitionId.size()));

        }
        else {
            partitionId = pathIdToPartitionId.get(pathId);
        }

        try {
            return new ProducerRecord<>(
                    topic,
                    ByteBuffer.allocate(4).putInt(partitionId).array(),
                    Strings.toByteArray(String.format("%d-%d", element.getSeqNum(), element.getValue()))
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Could not serialize record: " + element, e);
        }
    }
}
