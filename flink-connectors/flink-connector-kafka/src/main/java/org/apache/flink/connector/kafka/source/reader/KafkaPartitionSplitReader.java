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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.util.Collector;
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

/**
 * A {@link SplitReader} implementation that reads records from Kafka partitions.
 *
 * <p>The returned type are in the format of {@code tuple3(record, offset and timestamp}.
 *
 * @param <T> the type of the record to be emitted from the Source.
 */
public class KafkaPartitionSplitReader<T>
        implements SplitReader<Tuple3<T, Long, Long>, KafkaPartitionSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaPartitionSplitReader.class);
    private static final long POLL_TIMEOUT = 10000L;

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final Map<TopicPartition, Long> stoppingOffsets;
    private final SimpleCollector<T> collector;
    private final String groupId;
    private final int subtaskId;

    public KafkaPartitionSplitReader(
            Properties props,
            KafkaRecordDeserializationSchema<T> deserializationSchema,
            int subtaskId) {
        this.subtaskId = subtaskId;
        Properties consumerProps = new Properties();
        consumerProps.putAll(props);
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, createConsumerClientId(props));
        this.consumer = new KafkaConsumer<>(consumerProps);
        this.stoppingOffsets = new HashMap<>();
        this.deserializationSchema = deserializationSchema;
        this.collector = new SimpleCollector<>();
        this.groupId = consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    }

    @Override
    public RecordsWithSplitIds<Tuple3<T, Long, Long>> fetch() throws IOException {
        KafkaPartitionSplitRecords<Tuple3<T, Long, Long>> recordsBySplits =
                new KafkaPartitionSplitRecords<>();
        ConsumerRecords<byte[], byte[]> consumerRecords;
        try {
            consumerRecords = consumer.poll(Duration.ofMillis(POLL_TIMEOUT));
        } catch (WakeupException we) {
            recordsBySplits.prepareForRead();
            return recordsBySplits;
        }

        List<TopicPartition> finishedPartitions = new ArrayList<>();
        for (TopicPartition tp : consumerRecords.partitions()) {
            long stoppingOffset = getStoppingOffset(tp);
            String splitId = tp.toString();
            Collection<Tuple3<T, Long, Long>> recordsForSplit =
                    recordsBySplits.recordsForSplit(splitId);
            for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords.records(tp)) {
                // Stop consuming from this partition if the offsets has reached the stopping
                // offset.
                // Note that there are two cases, either case finishes a split:
                // 1. After processing a record with offset of "stoppingOffset - 1". The split
                // reader
                //    should not continue fetching because the record with stoppingOffset may not
                // exist.
                // 2. Before processing a record whose offset is greater than or equals to the
                // stopping
                //    offset. This should only happens when case 1 was not met due to log compaction
                // or
                //    log retention.
                // Case 2 is handled here. Case 1 is handled after the record is processed.
                if (consumerRecord.offset() >= stoppingOffset) {
                    finishSplitAtRecord(
                            tp,
                            stoppingOffset,
                            consumerRecord.offset(),
                            finishedPartitions,
                            recordsBySplits);
                    break;
                }
                // Add the record to the partition collector.
                try {
                    deserializationSchema.deserialize(consumerRecord, collector);
                    collector
                            .getRecords()
                            .forEach(
                                    r ->
                                            recordsForSplit.add(
                                                    new Tuple3<>(
                                                            r,
                                                            consumerRecord.offset(),
                                                            consumerRecord.timestamp())));
                    // Finish the split because there might not be any message after this point.
                    // Keep polling
                    // will just block forever.
                    if (consumerRecord.offset() == stoppingOffset - 1) {
                        finishSplitAtRecord(
                                tp,
                                stoppingOffset,
                                consumerRecord.offset(),
                                finishedPartitions,
                                recordsBySplits);
                    }
                } catch (Exception e) {
                    throw new IOException("Failed to deserialize consumer record due to", e);
                } finally {
                    collector.reset();
                }
            }
        }
        // Unassign the partitions that has finished.
        if (!finishedPartitions.isEmpty()) {
            unassignPartitions(finishedPartitions);
        }
        recordsBySplits.prepareForRead();
        return recordsBySplits;
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
        consumer.committed(partitionsStoppingAtCommitted)
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

    private void removeEmptySplits() {
        List<TopicPartition> emptySplits = new ArrayList<>();
        // If none of the partitions have any records,
        for (TopicPartition tp : consumer.assignment()) {
            if (consumer.position(tp) >= getStoppingOffset(tp)) {
                emptySplits.add(tp);
            }
        }
        if (!emptySplits.isEmpty()) {
            unassignPartitions(emptySplits);
        }
    }

    private void maybeLogSplitChangesHandlingResult(
            SplitsChange<KafkaPartitionSplit> splitsChange) {
        if (LOG.isDebugEnabled()) {
            StringJoiner splitsInfo = new StringJoiner(",");
            for (KafkaPartitionSplit split : splitsChange.splits()) {
                long startingOffset = consumer.position(split.getTopicPartition());
                long stoppingOffset = getStoppingOffset(split.getTopicPartition());
                splitsInfo.add(
                        String.format(
                                "[%s, start:%d, stop: %d]",
                                split.getTopicPartition(), startingOffset, stoppingOffset));
            }
            LOG.debug("SplitsChange handling result: {}", splitsInfo.toString());
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
            KafkaPartitionSplitRecords<Tuple3<T, Long, Long>> recordsBySplits) {
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

    // ---------------- private helper class ------------------------

    private static class KafkaPartitionSplitRecords<T> implements RecordsWithSplitIds<T> {
        private final Map<String, Collection<T>> recordsBySplits;
        private final Set<String> finishedSplits;
        private Iterator<Map.Entry<String, Collection<T>>> splitIterator;
        private String currentSplitId;
        private Iterator<T> recordIterator;

        private KafkaPartitionSplitRecords() {
            this.recordsBySplits = new HashMap<>();
            this.finishedSplits = new HashSet<>();
        }

        private Collection<T> recordsForSplit(String splitId) {
            return recordsBySplits.computeIfAbsent(splitId, id -> new ArrayList<>());
        }

        private void addFinishedSplit(String splitId) {
            finishedSplits.add(splitId);
        }

        private void prepareForRead() {
            splitIterator = recordsBySplits.entrySet().iterator();
        }

        @Override
        @Nullable
        public String nextSplit() {
            if (splitIterator.hasNext()) {
                Map.Entry<String, Collection<T>> entry = splitIterator.next();
                currentSplitId = entry.getKey();
                recordIterator = entry.getValue().iterator();
                return currentSplitId;
            } else {
                currentSplitId = null;
                recordIterator = null;
                return null;
            }
        }

        @Override
        @Nullable
        public T nextRecordFromSplit() {
            Preconditions.checkNotNull(
                    currentSplitId,
                    "Make sure nextSplit() did not return null before "
                            + "iterate over the records split.");
            if (recordIterator.hasNext()) {
                return recordIterator.next();
            } else {
                return null;
            }
        }

        @Override
        public Set<String> finishedSplits() {
            return finishedSplits;
        }
    }

    private static class SimpleCollector<T> implements Collector<T> {
        private final List<T> records = new ArrayList<>();

        @Override
        public void collect(T record) {
            records.add(record);
        }

        @Override
        public void close() {}

        private List<T> getRecords() {
            return records;
        }

        private void reset() {
            records.clear();
        }
    }
}
