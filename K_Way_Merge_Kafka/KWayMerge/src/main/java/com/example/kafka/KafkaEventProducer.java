package com.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.HashMap;

/**
 * Hello world!
 *
 */
public class KafkaEventProducer
{
    public static final int PARTITION_COUNT = 3;

    public static void main( String[] args )
    {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker addresses
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        // Consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Replace with your Kafka broker addresses
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id"); // Consumer group ID
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(producerProps);
        // Create Kafka consumer
        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("test_topic")); // Replace with your topic name
        ArrayList<Integer>[] perPartitionData = new ArrayList[PARTITION_COUNT]; // Create an array of ArrayLists with size 5

        for (int i = 0; i < perPartitionData.length; i++) {
            perPartitionData[i] = new ArrayList<>();
        }

        try {
            int totalMessages = 10;
            // Generate events and send them to Kafka
            // sendUniqueKeyMessages(producer, totalMessages);
            // sendLimitedKeyMessages(producer, totalMessages, PARTITION_COUNT);

            sendSingleKeyMessage(producer, totalMessages);
            Thread.sleep(5000); // delay before consuming

            int recieveCount = 0;
            int pollCount = 0;
            while (recieveCount < totalMessages || pollCount > 20) {
                System.out.println("Attempting to poll server");
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

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Closing Consumer and producer");
            producer.close();
            consumer.close();
        }
    }

    public static void sendUniqueKeyMessages(Producer<String, String> producer, int numMessages) {
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;
            String value = i + "";
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
            System.out.println("Sent Key: " + key + " value: " + value);
            producer.send(record);
        }
    }
    public static void sendLimitedKeyMessages(Producer<String, String> producer, int numMessages, int numKeys ) {
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + (i % numKeys);
            String value = i + "";
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
            System.out.println("Sent Key: " + key + " value: " + value);
            producer.send(record);
        }
    }
    public static void sendSingleKeyMessage(Producer<String, String> producer, int numMessages) {
        for (int i = 0; i < numMessages; i++) {
            String key = "test_key" ;
            String value = i + "";
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
            System.out.println("Sent Key: " + key + " value: " + value);
            producer.send(record);
        }
    }
}
