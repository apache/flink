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

import org.apache.flink.connector.kafka.testutils.annotations.Kafka;
import org.apache.flink.connector.kafka.testutils.annotations.KafkaKit;
import org.apache.flink.connector.kafka.testutils.extension.KafkaClientKit;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(TestLoggerExtension.class)
@Kafka
class FlinkKafkaInternalProducerITCase {

    private static final String TRANSACTION_PREFIX = "test-transaction-";

    @KafkaKit static KafkaClientKit kafkaClientKit;

    @Test
    void testInitTransactionId() {
        final String topic = "test-init-transactions";
        try (FlinkKafkaInternalProducer<String, String> reuse =
                new FlinkKafkaInternalProducer<>(getProducerIdempotenceProps(), "dummy")) {
            int numTransactions = 20;
            for (int i = 1; i <= numTransactions; i++) {
                reuse.initTransactionId(TRANSACTION_PREFIX + i);
                reuse.beginTransaction();
                reuse.send(new ProducerRecord<>(topic, "test-value-" + i));
                if (i % 2 == 0) {
                    reuse.commitTransaction();
                } else {
                    reuse.flush();
                    reuse.abortTransaction();
                }
                assertNumTransactions(i);
                assertThat(readRecords(topic).count()).isEqualTo(i / 2);
            }
        }
    }

    @ParameterizedTest
    @MethodSource("provideTransactionsFinalizer")
    void testResetInnerTransactionIfFinalizingTransactionFailed(
            Consumer<FlinkKafkaInternalProducer<?, ?>> transactionFinalizer) {
        final String topic = "reset-producer-internal-state";
        try (FlinkKafkaInternalProducer<String, String> fenced =
                new FlinkKafkaInternalProducer<>(getProducerIdempotenceProps(), "dummy")) {
            fenced.initTransactions();
            fenced.beginTransaction();
            fenced.send(new ProducerRecord<>(topic, "test-value"));
            // Start a second producer that fences the first one
            try (FlinkKafkaInternalProducer<String, String> producer =
                    new FlinkKafkaInternalProducer<>(getProducerIdempotenceProps(), "dummy")) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, "test-value"));
                producer.commitTransaction();
            }
            assertThatThrownBy(() -> transactionFinalizer.accept(fenced))
                    .isInstanceOf(ProducerFencedException.class);
            // Internal transaction should be reset and setting a new transactional id is possible
            fenced.setTransactionId("dummy2");
        }
    }

    private static Properties getProducerIdempotenceProps() {
        Properties properties =
                kafkaClientKit.createProducerProps(StringSerializer.class, StringSerializer.class);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        return properties;
    }

    private static List<Consumer<FlinkKafkaInternalProducer<?, ?>>> provideTransactionsFinalizer() {
        return Lists.newArrayList(
                FlinkKafkaInternalProducer::commitTransaction,
                FlinkKafkaInternalProducer::abortTransaction);
    }

    private void assertNumTransactions(int numTransactions) {
        List<KafkaTransactionLog.TransactionRecord> transactions =
                new KafkaTransactionLog(
                                kafkaClientKit.createConsumerProps(
                                        ByteArrayDeserializer.class, ByteArrayDeserializer.class))
                        .getTransactions(id -> id.startsWith(TRANSACTION_PREFIX));
        assertThat(
                        transactions.stream()
                                .map(KafkaTransactionLog.TransactionRecord::getTransactionId)
                                .collect(Collectors.toSet()))
                .hasSize(numTransactions);
    }

    private ConsumerRecords<String, String> readRecords(String topic) {
        Properties properties =
                kafkaClientKit.createConsumerProps(
                        StringDeserializer.class, StringDeserializer.class);
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
