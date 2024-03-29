package org.example;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class CustomKafkaPartitioner extends DefaultPartitioner {
    private HashMap<String, Integer> partitionMap = new HashMap<>();
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster, int numPartitions) {
        String keyValue = new String(keyBytes, StandardCharsets.UTF_8);
        if (partitionMap.containsKey(keyValue)){
            return partitionMap.get(keyValue);
        }
        else {
            int pId = partitionMap.size() % numPartitions;
            partitionMap.put(keyValue, pId);
            return pId;
        }

    }
}
