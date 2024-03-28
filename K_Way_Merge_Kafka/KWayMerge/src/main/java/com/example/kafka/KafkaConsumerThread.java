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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaConsumerThread implements Runnable{
        private volatile boolean running = true;
        private int partitionCount;
        Consumer<String, String> consumer;
        private ConcurrentLinkedQueue<kafkaMessage> partitionQueue [];
        String pattern = "(\\d+)-(\\d+)"; // detects things in the pattern %d-%d
        Pattern regex;
        Matcher matcher;


    public KafkaConsumerThread(String bootstrapServer , int partitionCount, ConcurrentLinkedQueue<kafkaMessage>[] queue) {
            this.partitionCount = partitionCount;
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer); // Replace with your Kafka broker addresses
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id"); // Consumer group ID
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            this.consumer = new KafkaConsumer<>(consumerProps);
            this.consumer.subscribe(Collections.singletonList("test_topic")); // Replace with your topic name
            this.partitionQueue = queue;
            this.regex = Pattern.compile(pattern);
        }
        @Override
        public void run() {
            while (running) {
                int pollTimeMilli = 1000;
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(
                        pollTimeMilli));
                for (ConsumerRecord<String, String> record : records) {
                    this.matcher = regex.matcher(record.value());
                    if(matcher.matches()){
                        int seqNum = Integer.parseInt(matcher.group(1));
                        int value = Integer.parseInt(matcher.group(2));
                        kafkaMessage message = new kafkaMessage(seqNum, value, System.nanoTime());
                        partitionQueue[record.partition()].offer(message);
                    }
                    else {
                        System.out.println("data is not in correct format closing");
                        stopRunning();
                    }
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
