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

package org.apache.flink.tests.util.kafka;

import org.apache.flink.api.common.time.Deadline;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/** A utility class that exposes common methods over a {@link KafkaContainer}. */
public class KafkaContainerClient {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerClient.class);
    private final KafkaContainer container;

    public KafkaContainerClient(KafkaContainer container) {
        this.container = container;
    }

    public void createTopic(int replicationFactor, int numPartitions, String topic) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(
                    Collections.singletonList(
                            new NewTopic(topic, numPartitions, (short) replicationFactor)));
        }
    }

    public <T> void sendMessages(String topic, Serializer<T> valueSerializer, T... messages) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (Producer<Bytes, T> producer =
                new KafkaProducer<>(props, new BytesSerializer(), valueSerializer)) {
            for (T message : messages) {
                producer.send(new ProducerRecord<>(topic, message));
            }
        }
    }

    public <T> List<T> readMessages(
            int expectedNumMessages,
            String groupId,
            String topic,
            Deserializer<T> valueDeserializer)
            throws IOException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, container.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final List<T> messages = Collections.synchronizedList(new ArrayList<>(expectedNumMessages));
        try (Consumer<Bytes, T> consumer =
                new KafkaConsumer<>(props, new BytesDeserializer(), valueDeserializer)) {
            consumer.subscribe(Pattern.compile(topic));
            final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(120));
            while (deadline.hasTimeLeft() && messages.size() < expectedNumMessages) {
                LOG.info(
                        "Waiting for messages. Received {}/{}.",
                        messages.size(),
                        expectedNumMessages);
                ConsumerRecords<Bytes, T> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<Bytes, T> record : records) {
                    messages.add(record.value());
                }
            }
            if (messages.size() != expectedNumMessages) {
                throw new IOException("Could not read expected number of messages.");
            }
            return messages;
        }
    }
}
