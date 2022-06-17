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

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** A {@link SplitReader} implementation that reads records from Kafka partitions. */
public class KafkaPartitionSplitReader
        implements SplitReader<ConsumerRecord<byte[], byte[]>, KafkaPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionSplitReader.class);
    private static final long POLL_TIMEOUT = 10000L;

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Map<TopicPartition, Long> stoppingOffsets;
    private final String groupId;
    private final int subtaskId;

    private final KafkaSourceReaderMetrics kafkaSourceReaderMetrics;

    // Tracking empty splits that has not been added to finished splits in fetch()
    private final Set<String> emptySplits = new HashSet<>();

    public KafkaPartitionSplitReader(
            Properties props,
            SourceReaderContext context,
            KafkaSourceReaderMetrics kafkaSourceReaderMetrics) {
        this.subtaskId = context.getIndexOfSubtask();
        this.kafkaSourceReaderMetrics = kafkaSourceReaderMetrics;
        Properties consumerProps = new Properties();
        consumerProps.putAll(props);
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, createConsumerClientId(props));
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.stoppingOffsets = new HashMap<>();
        this.groupId = consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);

        // Metric registration
        maybeRegisterKafkaConsumerMetrics(props, kafkaSourceReaderMetrics, consumer);
        this.kafkaSourceReaderMetrics.registerNumBytesIn(consumer);
    }

    @Override
    public RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> fetch() throws IOException {
        ConsumerRecords<byte[], byte[]> consumerRecords;
        try {
            consumerRecords = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
        } catch (WakeupException | IllegalStateException e) {
            // IllegalStateException will be thrown if the consumer is not assigned any partitions.
            // This happens if all assigned partitions are invalid or empty (starting offset >=
            // stopping offset). We just mark empty partitions as finished and return an empty
            // record container, and this consumer will be closed by SplitFetcherManager.
            KafkaPartitionSplitRecords recordsBySplits =
                    new KafkaPartitionSplitRecords(
                            ConsumerRecords.empty(), kafkaSourceReaderMetrics);
            markEmptySplitsAsFinished(recordsBySplits);
            return recordsBySplits;
        }
        KafkaPartitionSplitRecords recordsBySplits =
                new KafkaPartitionSplitRecords(consumerRecords, kafkaSourceReaderMetrics);
        List<TopicPartition> finishedPartitions = new ArrayList<>();
        for (TopicPartition tp : consumerRecords.partitions()) {
            long stoppingOffset = getStoppingOffset(tp);
            final List<ConsumerRecord<byte[], byte[]>> recordsFromPartition =
                    consumerRecords.records(tp);

            if (recordsFromPartition.size() > 0) {
                final ConsumerRecord<byte[], byte[]> lastRecord =
                        recordsFromPartition.get(recordsFromPartition.size() - 1);

                // After processing a record with offset of "stoppingOffset - 1", the split reader
                // should not continue fetching because the record with stoppingOffset may not
                // exist. Keep polling will just block forever.
                if (lastRecord.offset() >= stoppingOffset - 1) {
                    recordsBySplits.setPartitionStoppingOffset(tp, stoppingOffset);
                    finishSplitAtRecord(
                            tp,
                            stoppingOffset,
                            lastRecord.offset(),
                            finishedPartitions,
                            recordsBySplits);
                }
            }
            // Track this partition's record lag if it never appears before
            kafkaSourceReaderMetrics.maybeAddRecordsLagMetric(consumer, tp);
        }

        markEmptySplitsAsFinished(recordsBySplits);

        // Unassign the partitions that has finished.
        if (!finishedPartitions.isEmpty()) {
            finishedPartitions.forEach(kafkaSourceReaderMetrics::removeRecordsLagMetric);
            unassignPartitions(finishedPartitions);
        }

        // Update numBytesIn
        kafkaSourceReaderMetrics.updateNumBytesInCounter();

        return recordsBySplits;
    }

    private void markEmptySplitsAsFinished(KafkaPartitionSplitRecords recordsBySplits) {
        // Some splits are discovered as empty when handling split additions. These splits should be
        // added to finished splits to clean up states in split fetcher and source reader.
        if (!emptySplits.isEmpty()) {
            recordsBySplits.finishedSplits.addAll(emptySplits);
            emptySplits.clear();
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<KafkaPartitionSplit> splitsChange) {
        // Get all the partition assignments and stopping offsets.
        if (!(splitsChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChange.getClass()));
        }

        // Assignment.
        List<TopicPartition> newPartitionAssignments = new ArrayList<>();
        // Starting offsets.
        Map<TopicPartition, Long> partitionsStartingFromSpecifiedOffsets = new HashMap<>();
        List<TopicPartition> partitionsStartingFromEarliest = new ArrayList<>();
        List<TopicPartition> partitionsStartingFromLatest = new ArrayList<>();
        // Stopping offsets.
        List<TopicPartition> partitionsStoppingAtLatest = new ArrayList<>();
        Set<TopicPartition> partitionsStoppingAtCommitted = new HashSet<>();

        // Parse the starting and stopping offsets.
        splitsChange
                .splits()
                .forEach(
                        s -> {
                            newPartitionAssignments.add(s.getTopicPartition());
                            parseStartingOffsets(
                                    s,
                                    partitionsStartingFromEarliest,
                                    partitionsStartingFromLatest,
                                    partitionsStartingFromSpecifiedOffsets);
                            parseStoppingOffsets(
                                    s, partitionsStoppingAtLatest, partitionsStoppingAtCommitted);
                            // Track the new topic partition in metrics
                            kafkaSourceReaderMetrics.registerTopicPartition(s.getTopicPartition());
                        });

        // Assign new partitions.
        newPartitionAssignments.addAll(consumer.assignment());
        consumer.assign(newPartitionAssignments);

        // Seek on the newly assigned partitions to their stating offsets.
        seekToStartingOffsets(
                partitionsStartingFromEarliest,
                partitionsStartingFromLatest,
                partitionsStartingFromSpecifiedOffsets);
        // Setup the stopping offsets.
        acquireAndSetStoppingOffsets(partitionsStoppingAtLatest, partitionsStoppingAtCommitted);

        // After acquiring the starting and stopping offsets, remove the empty splits if necessary.
        removeEmptySplits();

        maybeLogSplitChangesHandlingResult(splitsChange);
    }

    @Override
    public void wakeUp() {
        consumer.wakeup();
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }

    // ---------------

    public void notifyCheckpointComplete(
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
            OffsetCommitCallback offsetCommitCallback) {
        consumer.commitAsync(offsetsToCommit, offsetCommitCallback);
    }

    // --------------- private helper method ----------------------

    private void parseStartingOffsets(
            KafkaPartitionSplit split,
            List<TopicPartition> partitionsStartingFromEarliest,
            List<TopicPartition> partitionsStartingFromLatest,
            Map<TopicPartition, Long> partitionsStartingFromSpecifiedOffsets) {
        TopicPartition tp = split.getTopicPartition();
        // Parse starting offsets.
        if (split.getStartingOffset() == KafkaPartitionSplit.EARLIEST_OFFSET) {
            partitionsStartingFromEarliest.add(tp);
        } else if (split.getStartingOffset() == KafkaPartitionSplit.LATEST_OFFSET) {
            partitionsStartingFromLatest.add(tp);
        } else if (split.getStartingOffset() == KafkaPartitionSplit.COMMITTED_OFFSET) {
            // Do nothing here, the consumer will first try to get the committed offsets of
            // these partitions by default.
        } else {
            partitionsStartingFromSpecifiedOffsets.put(tp, split.getStartingOffset());
        }
    }

    private void parseStoppingOffsets(
            KafkaPartitionSplit split,
            List<TopicPartition> partitionsStoppingAtLatest,
            Set<TopicPartition> partitionsStoppingAtCommitted) {
        TopicPartition tp = split.getTopicPartition();
        split.getStoppingOffset()
                .ifPresent(
                        stoppingOffset -> {
                            if (stoppingOffset >= 0) {
                                stoppingOffsets.put(tp, stoppingOffset);
                            } else if (stoppingOffset == KafkaPartitionSplit.LATEST_OFFSET) {
                                partitionsStoppingAtLatest.add(tp);
                            } else if (stoppingOffset == KafkaPartitionSplit.COMMITTED_OFFSET) {
                                partitionsStoppingAtCommitted.add(tp);
                            } else {
                                // This should not happen.
                                throw new FlinkRuntimeException(
                                        String.format(
                                                "Invalid stopping offset %d for partition %s",
                                                stoppingOffset, tp));
                            }
                        });
    }

    private void seekToStartingOffsets(
            List<TopicPartition> partitionsStartingFromEarliest,
            List<TopicPartition> partitionsStartingFromLatest,
            Map<TopicPartition, Long> partitionsStartingFromSpecifiedOffsets) {

        if (!partitionsStartingFromEarliest.isEmpty()) {
            LOG.trace("Seeking starting offsets to beginning: {}", partitionsStartingFromEarliest);
            consumer.seekToBeginning(partitionsStartingFromEarliest);
        }

        if (!partitionsStartingFromLatest.isEmpty()) {
            LOG.trace("Seeking starting offsets to end: {}", partitionsStartingFromLatest);
            consumer.seekToEnd(partitionsStartingFromLatest);
        }

        if (!partitionsStartingFromSpecifiedOffsets.isEmpty()) {
            LOG.trace(
                    "Seeking starting offsets to specified offsets: {}",
                    partitionsStartingFromSpecifiedOffsets);
            partitionsStartingFromSpecifiedOffsets.forEach(consumer::seek);
        }
    }

    private void acquireAndSetStoppingOffsets(
            List<TopicPartition> partitionsStoppingAtLatest,
            Set<TopicPartition> partitionsStoppingAtCommitted) {
        Map<TopicPartition, Long> endOffset = consumer.endOffsets(partitionsStoppingAtLatest);
        stoppingOffsets.putAll(endOffset);
        if (!partitionsStoppingAtCommitted.isEmpty()) {
            retryOnWakeup(
                            () -> consumer.committed(partitionsStoppingAtCommitted),
                            "getting committed offset as stopping offsets")
                    .forEach(
                            (tp, offsetAndMetadata) -> {
                                Preconditions.checkNotNull(
                                        offsetAndMetadata,
                                        String.format(
                                                "Partition %s should stop at committed offset. "
                                                        + "But there is no committed offset of this partition for group %s",
                                                tp, groupId));
                                stoppingOffsets.put(tp, offsetAndMetadata.offset());
                            });
        }
    }

    private void removeEmptySplits() {
        List<TopicPartition> emptyPartitions = new ArrayList<>();
        // If none of the partitions have any records,
        for (TopicPartition tp : consumer.assignment()) {
            if (retryOnWakeup(
                            () -> consumer.position(tp),
                            "getting starting offset to check if split is empty")
                    >= getStoppingOffset(tp)) {
                emptyPartitions.add(tp);
            }
        }
        if (!emptyPartitions.isEmpty()) {
            LOG.debug(
                    "These assigning splits are empty and will be marked as finished in later fetch: {}",
                    emptyPartitions);
            // Add empty partitions to empty split set for later cleanup in fetch()
            emptySplits.addAll(
                    emptyPartitions.stream()
                            .map(KafkaPartitionSplit::toSplitId)
                            .collect(Collectors.toSet()));
            // Un-assign partitions from Kafka consumer
            unassignPartitions(emptyPartitions);
        }
    }

    private void maybeLogSplitChangesHandlingResult(
            SplitsChange<KafkaPartitionSplit> splitsChange) {
        if (LOG.isDebugEnabled()) {
            StringJoiner splitsInfo = new StringJoiner(",");
            for (KafkaPartitionSplit split : splitsChange.splits()) {
                long startingOffset =
                        retryOnWakeup(
                                () -> consumer.position(split.getTopicPartition()),
                                "logging starting position");
                long stoppingOffset = getStoppingOffset(split.getTopicPartition());
                splitsInfo.add(
                        String.format(
                                "[%s, start:%d, stop: %d]",
                                split.getTopicPartition(), startingOffset, stoppingOffset));
            }
            LOG.debug("SplitsChange handling result: {}", splitsInfo);
        }
    }

    private void unassignPartitions(Collection<TopicPartition> partitionsToUnassign) {
        Collection<TopicPartition> newAssignment = new HashSet<>(consumer.assignment());
        newAssignment.removeAll(partitionsToUnassign);
        consumer.assign(newAssignment);
    }

    private String createConsumerClientId(Properties props) {
        String prefix = props.getProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key());
        return prefix + "-" + subtaskId;
    }

    private void finishSplitAtRecord(
            TopicPartition tp,
            long stoppingOffset,
            long currentOffset,
            List<TopicPartition> finishedPartitions,
            KafkaPartitionSplitRecords recordsBySplits) {
        LOG.debug(
                "{} has reached stopping offset {}, current offset is {}",
                tp,
                stoppingOffset,
                currentOffset);
        finishedPartitions.add(tp);
        recordsBySplits.addFinishedSplit(KafkaPartitionSplit.toSplitId(tp));
    }

    private long getStoppingOffset(TopicPartition tp) {
        return stoppingOffsets.getOrDefault(tp, Long.MAX_VALUE);
    }

    private void maybeRegisterKafkaConsumerMetrics(
            Properties props,
            KafkaSourceReaderMetrics kafkaSourceReaderMetrics,
            KafkaConsumer<?, ?> consumer) {
        final Boolean needToRegister =
                KafkaSourceOptions.getOption(
                        props,
                        KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS,
                        Boolean::parseBoolean);
        if (needToRegister) {
            kafkaSourceReaderMetrics.registerKafkaConsumerMetrics(consumer);
        }
    }

    /**
     * Catch {@link WakeupException} in Kafka consumer call and retry the invocation on exception.
     *
     * <p>This helper function handles a race condition as below:
     *
     * <ol>
     *   <li>Fetcher thread finishes a {@link KafkaConsumer#poll(Duration)} call
     *   <li>Task thread assigns new splits so invokes {@link #wakeUp()}, then the wakeup is
     *       recorded and held by the consumer
     *   <li>Later fetcher thread invokes {@link #handleSplitsChanges(SplitsChange)}, and
     *       interactions with consumer will throw {@link WakeupException} because of the previously
     *       held wakeup in the consumer
     * </ol>
     *
     * <p>Under this case we need to catch the {@link WakeupException} and retry the operation.
     */
    private <V> V retryOnWakeup(Supplier<V> consumerCall, String description) {
        try {
            return consumerCall.get();
        } catch (WakeupException we) {
            LOG.info(
                    "Caught WakeupException while executing Kafka consumer call for {}. Will retry the consumer call.",
                    description);
            return consumerCall.get();
        }
    }

    // ---------------- private helper class ------------------------

    private static class KafkaPartitionSplitRecords
            implements RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> {

        private final Set<String> finishedSplits = new HashSet<>();
        private final Map<TopicPartition, Long> stoppingOffsets = new HashMap<>();
        private final ConsumerRecords<byte[], byte[]> consumerRecords;
        private final KafkaSourceReaderMetrics metrics;
        private final Iterator<TopicPartition> splitIterator;
        private Iterator<ConsumerRecord<byte[], byte[]>> recordIterator;
        private TopicPartition currentTopicPartition;
        private Long currentSplitStoppingOffset;

        private KafkaPartitionSplitRecords(
                ConsumerRecords<byte[], byte[]> consumerRecords, KafkaSourceReaderMetrics metrics) {
            this.consumerRecords = consumerRecords;
            this.splitIterator = consumerRecords.partitions().iterator();
            this.metrics = metrics;
        }

        private void setPartitionStoppingOffset(
                TopicPartition topicPartition, long stoppingOffset) {
            stoppingOffsets.put(topicPartition, stoppingOffset);
        }

        private void addFinishedSplit(String splitId) {
            finishedSplits.add(splitId);
        }

        @Nullable
        @Override
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                currentTopicPartition = splitIterator.next();
                recordIterator = consumerRecords.records(currentTopicPartition).iterator();
                currentSplitStoppingOffset =
                        stoppingOffsets.getOrDefault(currentTopicPartition, Long.MAX_VALUE);
                return currentTopicPartition.toString();
            } else {
                currentTopicPartition = null;
                recordIterator = null;
                currentSplitStoppingOffset = null;
                return null;
            }
        }

        @Nullable
        @Override
        public ConsumerRecord<byte[], byte[]> nextRecordFromSplit() {
            Preconditions.checkNotNull(
                    currentTopicPartition,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (recordIterator.hasNext()) {
                final ConsumerRecord<byte[], byte[]> record = recordIterator.next();
                // Only emit records before stopping offset
                if (record.offset() < currentSplitStoppingOffset) {
                    metrics.recordCurrentOffset(currentTopicPartition, record.offset());
                    return record;
                }
            }
            return null;
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }
}
