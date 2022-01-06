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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.sink.KafkaUtil.drainAllRecordsFromTopic;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME;

/**
 * This class is responsible to provide the format of the used transationalIds and in case of an
 * application restart query the open transactions and decide which must be aborted.
 */
class KafkaTransactionLog {

    private static final int SUPPORTED_KAFKA_SCHEMA_VERSION = 0;
    private final Properties consumerConfig;

    /**
     * Constructor creating a KafkaTransactionLog.
     *
     * @param kafkaConfig used to configure the {@link KafkaConsumer} to query the topic containing
     *     the transaction information
     */
    KafkaTransactionLog(Properties kafkaConfig) {
        this.consumerConfig = new Properties();
        consumerConfig.putAll(checkNotNull(kafkaConfig, "kafkaConfig"));
        consumerConfig.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put("enable.auto.commit", false);
    }

    public List<TransactionRecord> getTransactions() {
        return getTransactions(id -> true);
    }

    /** Gets all {@link TransactionRecord} matching the given id filter. */
    public List<TransactionRecord> getTransactions(Predicate<String> transactionIdFilter)
            throws KafkaException {
        return drainAllRecordsFromTopic(TRANSACTION_STATE_TOPIC_NAME, consumerConfig, true).stream()
                .map(r -> parseTransaction(r, transactionIdFilter))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    private Optional<TransactionRecord> parseTransaction(
            ConsumerRecord<byte[], byte[]> consumerRecord, Predicate<String> transactionIdFilter) {
        final ByteBuffer keyBuffer = ByteBuffer.wrap(consumerRecord.key());
        checkKafkaSchemaVersionMatches(keyBuffer);
        // Ignore 2 bytes because Kafka's internal representation
        keyBuffer.getShort();
        final String transactionalId = StandardCharsets.UTF_8.decode(keyBuffer).toString();

        if (!transactionIdFilter.test(transactionalId)) {
            return Optional.empty();
        }

        final ByteBuffer valueBuffer = ByteBuffer.wrap(consumerRecord.value());
        checkKafkaSchemaVersionMatches(valueBuffer);
        final TransactionState state = TransactionState.fromByte(readTransactionState(valueBuffer));

        return Optional.of(new TransactionRecord(transactionalId, state));
    }

    private static byte readTransactionState(ByteBuffer buffer) {
        // producerId
        buffer.getLong();
        // epoch
        buffer.getShort();
        // transactionTimeout
        buffer.getInt();
        // statusKey
        return buffer.get();
    }

    public static class TransactionRecord {
        private final String transactionId;
        private final TransactionState state;

        public TransactionRecord(String transactionId, TransactionState state) {
            this.transactionId = checkNotNull(transactionId);
            this.state = checkNotNull(state);
        }

        public String getTransactionId() {
            return transactionId;
        }

        public TransactionState getState() {
            return state;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TransactionRecord that = (TransactionRecord) o;
            return transactionId.equals(that.transactionId) && state == that.state;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transactionId, state);
        }

        @Override
        public String toString() {
            return "TransactionRecord{"
                    + "transactionId='"
                    + transactionId
                    + '\''
                    + ", state="
                    + state
                    + '}';
        }
    }

    public enum TransactionState {
        Empty(Byte.parseByte("0"), false),
        Ongoing(Byte.parseByte("1"), false),
        PrepareCommit(Byte.parseByte("2"), false),
        PrepareAbort(Byte.parseByte("3"), false),
        CompleteCommit(Byte.parseByte("4"), true),
        CompleteAbort(Byte.parseByte("5"), true),
        Dead(Byte.parseByte("6"), true),
        PrepareEpochFence(Byte.parseByte("7"), false);

        private static final Map<Byte, TransactionState> BYTE_TO_STATE =
                Arrays.stream(TransactionState.values())
                        .collect(Collectors.toMap(e -> e.state, e -> e));

        private final byte state;

        private boolean terminal;

        TransactionState(byte state, boolean terminal) {
            this.state = state;
            this.terminal = terminal;
        }

        public boolean isTerminal() {
            return terminal;
        }

        static TransactionState fromByte(byte state) {
            final TransactionState transactionState = BYTE_TO_STATE.get(state);
            if (transactionState == null) {
                throw new IllegalArgumentException(
                        String.format("The given state %s is not supported.", state));
            }
            return transactionState;
        }
    }

    private static void checkKafkaSchemaVersionMatches(ByteBuffer buffer) {
        final short version = buffer.getShort();
        if (version != SUPPORTED_KAFKA_SCHEMA_VERSION) {
            throw new IllegalStateException(
                    String.format(
                            "Kafka has changed the schema version from %s to %s",
                            SUPPORTED_KAFKA_SCHEMA_VERSION, version));
        }
    }
}
