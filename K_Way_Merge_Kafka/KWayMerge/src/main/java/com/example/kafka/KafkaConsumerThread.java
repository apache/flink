package com.example.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;

public class KafkaConsumerThread implements Runnable{
        private volatile boolean running = true;
        private int partitionCount;
        Consumer<String, String> consumer;
        private ConcurrentLinkedQueue<Integer> partitionQueue [];

        public KafkaConsumerThread(String bootstrapServer , int partitionCount, ConcurrentLinkedQueue<Integer>[] queue) {
            this.partitionCount = partitionCount;
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer); // Replace with your Kafka broker addresses
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id"); // Consumer group ID
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            this.consumer = new KafkaConsumer<>(consumerProps);
            this.consumer.subscribe(Collections.singletonList("test_topic")); // Replace with your topic name
            this.partitionQueue = queue;
        }
        @Override
        public void run() {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    partitionQueue[record.partition()].offer(Integer.parseInt(record.value()));
                }
                consumer.commitSync();
            }
            close();
        }
        public void stopRunning() {
            running = false;
        }
        public void close() {
            this.consumer.close();
        }
}
