package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.awt.windows.ThemeReader;

import java.util.Properties;

public class KafkaTestProducer {
    public int PARTITION_COUNT;
    Producer<String, String> producer;

    public boolean isRunning = true;

    public KafkaTestProducer(String bootstrapServer, int numPartitions) {
        this.PARTITION_COUNT = numPartitions;
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServer); // Replace with your Kafka broker addresses
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(producerProps);
    }

    public void sendUniqueKeyMessages(int numMessages) {
        for (int i = 0; i < numMessages; i++) {
            String key = "key-" + i;
            String value = i + "";
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
            System.out.println("Sent Key: " + key + " value: " + value);
            producer.send(record);
        }
    }

    public void sendLimitedKeyMessages(int numMessages, int numKeys, int offset) {
        for (int i = offset; i < numMessages + offset; i++) {
            String key = "key-" + (i % numKeys);
            String value = i + "";
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
            System.out.println("Sent Key: " + key + " value: " + value);
            producer.send(record);
        }
    }

    public void sendSingleKeyMessage(int numMessages) {
        for (int i = 0; i < numMessages; i++) {
            String key = "test_key";
            String value = i + "";
            ProducerRecord<String, String> record = new ProducerRecord<>("test_topic", key, value);
            System.out.println("Sent Key: " + key + " value: " + value);
            producer.send(record);
        }
    }

    public void close() {
        this.producer.close();
    }
}
