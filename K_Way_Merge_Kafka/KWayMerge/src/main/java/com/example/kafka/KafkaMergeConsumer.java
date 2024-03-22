package com.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

public class KafkaMergeConsumer {
    int PARTITION_COUNT;
    Consumer<String, String> consumer;

    ArrayList<Integer>[] perPartitionData;

    public KafkaMergeConsumer(String bootstrapServer , int partitionCount) {
        this.PARTITION_COUNT = partitionCount;
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer); // Replace with your Kafka broker addresses
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id"); // Consumer group ID
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.consumer.subscribe(Collections.singletonList("test_topic")); // Replace with your topic name
        this.perPartitionData = new ArrayList[PARTITION_COUNT];

        for (int i = 0; i < perPartitionData.length; i++) {
            perPartitionData[i] = new ArrayList<>();
        }
    }

    public ArrayList<Integer> consumeMergeLimitedMessages(int totalMessages) {
            int recieveCount = 0;
            int pollCount = 0;
            while (recieveCount < totalMessages || pollCount > 20) {
                System.out.println("Attempting to poll server with poll count " + pollCount);
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
//                    System.out.println("Partition " + record.partition() + " Key: " + record.key());
                    perPartitionData[record.partition()].add(Integer.parseInt(record.value()));
                    recieveCount++;
                }
                consumer.commitSync();
                pollCount++;
            }

        for (int i = 0; i < perPartitionData.length; i++){
            ArrayList<Integer> currList = perPartitionData[i];
            System.out.println("Partition: " + i + " " + currList.toString());
        }
        ArrayList<Integer> res = Merge.merge(perPartitionData);
        System.out.println("Sorted Array: " + res.toString());

        return res;
    }
    public void close(){
        this.consumer.close();
    }
}
