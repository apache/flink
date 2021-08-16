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

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests for {@link KafkaTransactionLog} to retrieve abortable Kafka transactions. */
public class KafkaTransactionLogITCase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkITCase.class);
    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final String TOPIC_NAME = "kafkaTransactionLogTest";
    private static final String TRANSACTIONAL_ID_PREFIX = "kafka-log";

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"))
                    .withEmbeddedZookeeper()
                    .withEnv(
                            ImmutableMap.of(
                                    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                                    "1",
                                    "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                                    String.valueOf(Duration.ofHours(2).toMillis()),
                                    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR",
                                    "1",
                                    "KAFKA_MIN_INSYNC_REPLICAS",
                                    "1"))
                    .withNetwork(NETWORK)
                    .withLogConsumer(LOG_CONSUMER)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    private final List<Producer<byte[], Integer>> openProducers = new ArrayList<>();

    @After
    public void tearDown() {
        openProducers.forEach(Producer::close);
    }

    @Test
    public void testGetTransactionsToAbort() {
        committedTransaction(0, 1);
        abortedTransaction(0, 2);
        lingeringTransaction(0, 3);
        lingeringTransaction(0, 4);

        committedTransaction(2, 1);
        lingeringTransaction(2, 2);

        committedTransaction(3, 1);
        lingeringTransaction(3, 2);

        committedTransaction(5, 1);
        lingeringTransaction(5, 2);

        committedTransaction(6, 1);

        lingeringTransaction(7, 1);
        lingeringTransaction(8, 1);

        try (final KafkaTransactionLog transactionLog =
                new KafkaTransactionLog(
                        getKafkaClientConfiguration(),
                        createWriterState(0, 1),
                        ImmutableList.of(createWriterState(5, 1), createWriterState(2, 0)),
                        2)) {
            final List<String> transactionsToAbort = transactionLog.getTransactionsToAbort();
            assertThat(
                    transactionsToAbort,
                    containsInAnyOrder(
                            buildTransactionalId(0, 3),
                            buildTransactionalId(0, 4),
                            buildTransactionalId(2, 2),
                            buildTransactionalId(5, 2),
                            buildTransactionalId(8, 1)));
        }

        try (final KafkaTransactionLog transactionLog =
                new KafkaTransactionLog(
                        getKafkaClientConfiguration(),
                        createWriterState(1, 1),
                        ImmutableList.of(createWriterState(6, 1), createWriterState(3, 1)),
                        2)) {
            final List<String> transactionsToAbort = transactionLog.getTransactionsToAbort();
            assertThat(
                    transactionsToAbort,
                    containsInAnyOrder(buildTransactionalId(3, 2), buildTransactionalId(7, 1)));
        }
    }

    private static KafkaWriterState createWriterState(int subtaskId, long offset) {
        return new KafkaWriterState(TRANSACTIONAL_ID_PREFIX, subtaskId, offset);
    }

    private void committedTransaction(int subtaskId, long offset) {
        submitTransaction(
                subtaskId,
                offset,
                producer -> {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(TOPIC_NAME, 0, null, null, 1));
                    producer.flush();
                    producer.commitTransaction();
                    producer.flush();
                });
    }

    private void lingeringTransaction(int subtaskId, long offset) {
        submitTransaction(
                subtaskId,
                offset,
                producer -> {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(TOPIC_NAME, 0, null, null, 1));
                    producer.flush();
                });
    }

    private void abortedTransaction(int subtaskId, long offset) {
        submitTransaction(
                subtaskId,
                offset,
                producer -> {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(TOPIC_NAME, 0, null, null, 1));
                    producer.flush();
                    producer.abortTransaction();
                    producer.flush();
                });
    }

    private void submitTransaction(
            int subtaskId, long offset, Consumer<Producer<byte[], Integer>> producerAction) {
        final Producer<byte[], Integer> producer =
                createProducer(buildTransactionalId(subtaskId, offset));
        openProducers.add(producer);
        producerAction.accept(producer);
    }

    private static String buildTransactionalId(int subtaskId, long offset) {
        return TransactionalIdFactory.buildTransactionalId(
                TRANSACTIONAL_ID_PREFIX, subtaskId, offset);
    }

    private static Producer<byte[], Integer> createProducer(String transactionalId) {
        final Properties producerProperties = getKafkaClientConfiguration();
        producerProperties.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerProperties.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        return new KafkaProducer<>(producerProperties);
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", "flink-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        return standardProps;
    }
}
