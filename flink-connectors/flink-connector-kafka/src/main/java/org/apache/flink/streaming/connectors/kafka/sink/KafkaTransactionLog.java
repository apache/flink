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

package org.apache.flink.streaming.connectors.kafka.sink;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.kafka.sink.KafkaTransactionLog.TransactionState.CompleteAbort;
import static org.apache.flink.streaming.connectors.kafka.sink.KafkaTransactionLog.TransactionState.CompleteCommit;
import static org.apache.flink.streaming.connectors.kafka.sink.KafkaTransactionLog.TransactionState.Dead;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.kafka.common.internals.Topic.TRANSACTION_STATE_TOPIC_NAME;

/**
 * This class is responsible to provide the format of the used transationalIds and in case of an
 * application restart query the open transactions and decide which must be aborted.
 */
class KafkaTransactionLog implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaTransactionLog.class);
    private static final Duration CONSUMER_POLL_DURATION = Duration.ofSeconds(1);
    private static final Set<TransactionState> TERMINAL_TRANSACTION_STATES =
            ImmutableSet.of(CompleteCommit, CompleteAbort, Dead);
    private static final int SUPPORTED_KAFKA_SCHEMA_VERSION = 0;

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaWriterState main;
    private final TransactionsToAbortChecker transactionToAbortChecker;

    /**
     * Constructor creating a KafkaTransactionLog.
     *
     * @param kafkaConfig used to configure the {@link KafkaConsumer} to query the topic containing
     *     the transaction information
     * @param main the {@link KafkaWriterState} which was previously snapshotted by this subtask
     * @param others the {@link KafkaWriterState}s which are from different subtasks i.e. in case of
     *     a scale-in
     * @param numberOfParallelSubtasks current number of parallel sink tasks
     */
    KafkaTransactionLog(
            Properties kafkaConfig,
            KafkaWriterState main,
            List<KafkaWriterState> others,
            int numberOfParallelSubtasks) {
        this.main = checkNotNull(main, "mainState");
        checkNotNull(others, "othersState");
        final Map<Integer, Long> subtaskIdCheckpointOffsetMapping =
                new ImmutableList.Builder<KafkaWriterState>()
                        .add(main).addAll(others).build().stream()
                                .collect(
                                        Collectors.toMap(
                                                KafkaWriterState::getSubtaskId,
                                                KafkaWriterState::getTransactionalIdOffset));
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(checkNotNull(kafkaConfig, "kafkaConfig"));
        consumerConfig.put("enable.auto.commit", false);
        consumerConfig.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put("auto.offset.reset", "earliest");
        consumerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        this.transactionToAbortChecker =
                new TransactionsToAbortChecker(
                        numberOfParallelSubtasks,
                        subtaskIdCheckpointOffsetMapping,
                        main.getSubtaskId());
        this.consumer = new KafkaConsumer<>(consumerConfig);
        this.consumer.subscribe(ImmutableList.of(TRANSACTION_STATE_TOPIC_NAME));
    }

    /**
     * This method queries Kafka's internal transaction topic and filters the transactions for the
     * following rules.
     * <li>transaction is in no terminal state {@link
     *     KafkaTransactionLog#TERMINAL_TRANSACTION_STATES}
     * <li>transactionalIdPrefix equals the one from {@link #main}
     * <li>Applies the rules from {@link TransactionsToAbortChecker}
     *
     * @return all transactionIds which must be aborted before starting new transactions.
     */
    public List<String> getTransactionsToAbort() {
        final Map<Integer, Map<Long, String>> openTransactions = new HashMap<>();
        final List<TopicPartition> partitions = getAllPartitions();
        final Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
        do {
            // We have to call poll() first before we have any partitions assigned
            ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_DURATION);
            records.records(TRANSACTION_STATE_TOPIC_NAME)
                    .forEach(maybeAddTransaction(openTransactions));
        } while (!hasReadAllRecords(endOffsets, partitions));

        return transactionToAbortChecker.getTransactionsToAbort(openTransactions);
    }

    private boolean hasReadAllRecords(
            Map<TopicPartition, Long> endOffsets, List<TopicPartition> partitions) {
        final ListIterator<TopicPartition> it = partitions.listIterator();
        while (it.hasNext()) {
            final TopicPartition partition = it.next();
            final long endOffset = endOffsets.get(partition);
            if (endOffset == 0 || consumer.position(partition) >= endOffset) {
                // Remove finished partitions to decrease the look-up space
                it.remove();
                continue;
            }
            return false;
        }
        return true;
    }

    private List<TopicPartition> getAllPartitions() {
        final List<PartitionInfo> partitionInfos =
                consumer.partitionsFor(TRANSACTION_STATE_TOPIC_NAME);
        return partitionInfos.stream()
                .filter(info -> info.topic().equals(TRANSACTION_STATE_TOPIC_NAME))
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toList());
    }

    private Consumer<ConsumerRecord<byte[], byte[]>> maybeAddTransaction(
            Map<Integer, Map<Long, String>> openTransactions) {
        return record -> {
            final ByteBuffer keyBuffer = ByteBuffer.wrap(record.key());
            checkKafkaSchemaVersionMatches(keyBuffer);
            // Ignore 2 bytes because Kafka's internal representation
            keyBuffer.getShort();
            final String transactionalId = StandardCharsets.UTF_8.decode(keyBuffer).toString();

            final Optional<KafkaWriterState> openTransactionOpt =
                    TransactionalIdFactory.parseKafkaWriterState(transactionalId);
            // If the transactionalId does not follow the format ignore it
            if (!openTransactionOpt.isPresent()) {
                return;
            }

            final KafkaWriterState openTransaction = openTransactionOpt.get();
            // If the transactionalId prefixes differ ignore
            if (!openTransaction
                    .getTransactionalIdPrefix()
                    .equals(main.getTransactionalIdPrefix())) {
                LOG.debug(
                        "The transactionalId prefixes differ. Open: {}, Recovered: {}",
                        openTransaction.getTransactionalIdPrefix(),
                        main.getTransactionalIdPrefix());
                return;
            }

            final ByteBuffer valueBuffer = ByteBuffer.wrap(record.value());
            checkKafkaSchemaVersionMatches(valueBuffer);
            final TransactionState state =
                    TransactionState.fromByte(readTransactionState(valueBuffer));

            LOG.debug("Transaction {} is in state {}", transactionalId, state);

            final int openSubtaskIndex = openTransaction.getSubtaskId();
            final long openCheckpointOffset = openTransaction.getTransactionalIdOffset();

            // If the transaction is in a final state ignore it
            if (isTransactionInFinalState(state)) {
                openTransactions.get(openSubtaskIndex).remove(openCheckpointOffset);
                return;
            }

            if (openTransactions.containsKey(openSubtaskIndex)) {
                openTransactions.get(openSubtaskIndex).put(openCheckpointOffset, transactionalId);
            } else {
                final Map<Long, String> map = new HashMap<>();
                map.put(openCheckpointOffset, transactionalId);
                openTransactions.put(openSubtaskIndex, map);
            }
        };
    }

    @Override
    public void close() {
        consumer.close();
    }

    private static boolean isTransactionInFinalState(TransactionState state) {
        return TERMINAL_TRANSACTION_STATES.contains(state);
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

    enum TransactionState {
        Empty(Byte.parseByte("0")),
        Ongoing(Byte.parseByte("1")),
        PrepareCommit(Byte.parseByte("2")),
        PrepareAbort(Byte.parseByte("3")),
        CompleteCommit(Byte.parseByte("4")),
        CompleteAbort(Byte.parseByte("5")),
        Dead(Byte.parseByte("6")),
        PrepareEpochFence(Byte.parseByte("7"));

        private static final Map<Byte, TransactionState> BYTE_TO_STATE =
                Arrays.stream(TransactionState.values())
                        .collect(Collectors.toMap(e -> e.state, e -> e));

        private final byte state;

        TransactionState(byte state) {
            this.state = state;
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
