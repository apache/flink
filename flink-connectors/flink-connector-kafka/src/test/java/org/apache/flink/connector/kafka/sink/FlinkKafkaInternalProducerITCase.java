/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@Testcontainers
class FlinkKafkaInternalProducerITCase extends TestLogger {

    private static final String TEST_TOPIC = "test-topic";
    private static final Logger LOG =
            LoggerFactory.getLogger(FlinkKafkaInternalProducerITCase.class);

    @Container
    private static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.2"))
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                    .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                    .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
                    .withLogConsumer(new Slf4jLogConsumer(LOG))
                    .withEmbeddedZookeeper();

    private static final String TRANSACTION_PREFIX = "test-transaction-";

    Properties getProperties() {
        Properties properties = new Properties();
        properties.put(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA_CONTAINER.getBootstrapServers());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return properties;
    }

    @Test
    void testInitTransactionId() {
        try (FlinkKafkaInternalProducer<String, String> reuse =
                new FlinkKafkaInternalProducer<>(getProperties(), "dummy")) {
            int numTransactions = 20;
            for (int i = 1; i <= numTransactions; i++) {
                reuse.initTransactionId(TRANSACTION_PREFIX + i);
                reuse.beginTransaction();
                reuse.send(new ProducerRecord<>(TEST_TOPIC, "test-value-" + i));
                if (i % 2 == 0) {
                    reuse.commitTransaction();
                } else {
                    reuse.flush();
                    reuse.abortTransaction();
                }
                assertNumTransactions(i);
                assertThat(readRecords(TEST_TOPIC).count(), equalTo(i / 2));
            }
        }
    }

    private void assertNumTransactions(int numTransactions) {
        List<KafkaTransactionLog.TransactionRecord> transactions =
                new KafkaTransactionLog(getProperties())
                        .getTransactions(id -> id.startsWith(TRANSACTION_PREFIX));
        assertThat(
                transactions.stream()
                        .map(KafkaTransactionLog.TransactionRecord::getTransactionId)
                        .collect(Collectors.toSet()),
                hasSize(numTransactions));
    }

    private ConsumerRecords<String, String> readRecords(String topic) {
        Properties properties = getProperties();
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.assign(
                consumer.partitionsFor(topic).stream()
                        .map(partitionInfo -> new TopicPartition(topic, partitionInfo.partition()))
                        .collect(Collectors.toSet()));
        consumer.seekToBeginning(consumer.assignment());
        return consumer.poll(Duration.ofMillis(1000));
    }
}
