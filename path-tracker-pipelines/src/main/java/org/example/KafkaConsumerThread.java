/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

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
                int pollTimeMilli = 100;
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
                        System.out.println("data is not in correct format closing " + record.value());
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
