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

import org.apache.flink.connector.kafka.sink.KafkaTransactionLog.TransactionRecord;
import org.apache.flink.util.TestLogger;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.connector.kafka.sink.KafkaTransactionLog.TransactionState.CompleteAbort;
import static org.apache.flink.connector.kafka.sink.KafkaTransactionLog.TransactionState.CompleteCommit;
import static org.apache.flink.connector.kafka.sink.KafkaTransactionLog.TransactionState.Empty;
import static org.apache.flink.connector.kafka.sink.KafkaTransactionLog.TransactionState.Ongoing;
import static org.apache.flink.connector.kafka.sink.KafkaTransactionLog.TransactionState.PrepareAbort;
import static org.apache.flink.connector.kafka.sink.KafkaTransactionLog.TransactionState.PrepareCommit;
import static org.apache.flink.connector.kafka.sink.KafkaUtil.createKafkaContainer;
import static org.apache.flink.util.DockerImageVersions.KAFKA;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests for {@link KafkaTransactionLog} to retrieve abortable Kafka transactions. */
public class KafkaTransactionLogITCase extends TestLogger {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkITCase.class);
    private static final String TOPIC_NAME = "kafkaTransactionLogTest";
    private static final String TRANSACTIONAL_ID_PREFIX = "kafka-log";

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KAFKA, LOG).withEmbeddedZookeeper();

    private final List<Producer<byte[], Integer>> openProducers = new ArrayList<>();

    @After
    public void tearDown() {
        openProducers.forEach(Producer::close);
    }

    @Test
    public void testGetTransactions() {
        committedTransaction(1);
        abortedTransaction(2);
        lingeringTransaction(3);
        lingeringTransaction(4);

        final KafkaTransactionLog transactionLog =
                new KafkaTransactionLog(getKafkaClientConfiguration());
        final List<TransactionRecord> transactions = transactionLog.getTransactions();
        assertThat(
                transactions,
                containsInAnyOrder(
                        new TransactionRecord(buildTransactionalId(1), Empty),
                        new TransactionRecord(buildTransactionalId(1), Ongoing),
                        new TransactionRecord(buildTransactionalId(1), PrepareCommit),
                        new TransactionRecord(buildTransactionalId(1), CompleteCommit),
                        new TransactionRecord(buildTransactionalId(2), Empty),
                        new TransactionRecord(buildTransactionalId(2), Ongoing),
                        new TransactionRecord(buildTransactionalId(2), PrepareAbort),
                        new TransactionRecord(buildTransactionalId(2), CompleteAbort),
                        new TransactionRecord(buildTransactionalId(3), Empty),
                        new TransactionRecord(buildTransactionalId(3), Ongoing),
                        new TransactionRecord(buildTransactionalId(4), Empty),
                        new TransactionRecord(buildTransactionalId(4), Ongoing)));
    }

    private void committedTransaction(long id) {
        submitTransaction(
                id,
                producer -> {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(TOPIC_NAME, 0, null, null, 1));
                    producer.flush();
                    producer.commitTransaction();
                    producer.flush();
                });
    }

    private void lingeringTransaction(long id) {
        submitTransaction(
                id,
                producer -> {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(TOPIC_NAME, 0, null, null, 1));
                    producer.flush();
                });
    }

    private void abortedTransaction(long id) {
        submitTransaction(
                id,
                producer -> {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>(TOPIC_NAME, 0, null, null, 1));
                    producer.flush();
                    producer.abortTransaction();
                    producer.flush();
                });
    }

    private void submitTransaction(long id, Consumer<Producer<byte[], Integer>> producerAction) {
        Producer<byte[], Integer> producer = createProducer(buildTransactionalId(id));
        openProducers.add(producer);
        producerAction.accept(producer);
        // don't close here for lingering transactions
    }

    private static String buildTransactionalId(long id) {
        return TRANSACTIONAL_ID_PREFIX + id;
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
        standardProps.put("auto.id.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        return standardProps;
    }
}
