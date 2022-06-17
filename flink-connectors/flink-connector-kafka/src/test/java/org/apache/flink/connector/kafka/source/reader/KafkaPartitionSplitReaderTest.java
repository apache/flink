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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.testutils.KafkaSourceTestEnv;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.connector.kafka.source.testutils.KafkaSourceTestEnv.NUM_RECORDS_PER_PARTITION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link KafkaPartitionSplitReader}. */
public class KafkaPartitionSplitReaderTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";

    private static Map<Integer, Map<String, KafkaPartitionSplit>> splitsByOwners;
    private static Map<TopicPartition, Long> earliestOffsets;

    private final IntegerDeserializer deserializer = new IntegerDeserializer();

    @BeforeAll
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.setupTopic(TOPIC1, true, true, KafkaSourceTestEnv::getRecordsForTopic);
        KafkaSourceTestEnv.setupTopic(TOPIC2, true, true, KafkaSourceTestEnv::getRecordsForTopic);
        splitsByOwners =
                KafkaSourceTestEnv.getSplitsByOwners(Arrays.asList(TOPIC1, TOPIC2), NUM_SUBTASKS);
        earliestOffsets =
                KafkaSourceTestEnv.getEarliestOffsets(
                        KafkaSourceTestEnv.getPartitionsForTopics(Arrays.asList(TOPIC1, TOPIC2)));
    }

    @AfterAll
    public static void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    @Test
    public void testHandleSplitChangesAndFetch() throws Exception {
        KafkaPartitionSplitReader reader = createReader();
        assignSplitsAndFetchUntilFinish(reader, 0);
        assignSplitsAndFetchUntilFinish(reader, 1);
    }

    @Test
    public void testWakeUp() throws Exception {
        KafkaPartitionSplitReader reader = createReader();
        TopicPartition nonExistingTopicPartition = new TopicPartition("NotExist", 0);
        assignSplits(
                reader,
                Collections.singletonMap(
                        KafkaPartitionSplit.toSplitId(nonExistingTopicPartition),
                        new KafkaPartitionSplit(nonExistingTopicPartition, 0)));
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread t =
                new Thread(
                        () -> {
                            try {
                                reader.fetch();
                            } catch (Throwable e) {
                                error.set(e);
                            }
                        },
                        "testWakeUp-thread");
        t.start();
        long deadline = System.currentTimeMillis() + 5000L;
        while (t.isAlive() && System.currentTimeMillis() < deadline) {
            reader.wakeUp();
            Thread.sleep(10);
        }
        assertNull(error.get());
    }

    @Test
    public void testWakeupThenAssign() throws IOException {
        KafkaPartitionSplitReader reader = createReader();
        // Assign splits with records
        assignSplits(reader, splitsByOwners.get(0));
        // Run a fetch operation, and it should not block
        reader.fetch();
        // Wake the reader up then assign a new split. This assignment should not throw
        // WakeupException.
        reader.wakeUp();
        TopicPartition tp = new TopicPartition(TOPIC1, 0);
        assignSplits(
                reader,
                Collections.singletonMap(
                        KafkaPartitionSplit.toSplitId(tp),
                        new KafkaPartitionSplit(tp, KafkaPartitionSplit.EARLIEST_OFFSET)));
    }

    @Test
    public void testNumBytesInCounter() throws Exception {
        final OperatorMetricGroup operatorMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
        final Counter numBytesInCounter =
                operatorMetricGroup.getIOMetricGroup().getNumBytesInCounter();
        KafkaPartitionSplitReader reader =
                createReader(
                        new Properties(),
                        InternalSourceReaderMetricGroup.wrap(operatorMetricGroup));
        // Add a split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaPartitionSplit(new TopicPartition(TOPIC1, 0), 0L))));
        reader.fetch();
        final long latestNumBytesIn = numBytesInCounter.getCount();
        // Since it's hard to know the exact number of bytes consumed, we just check if it is
        // greater than 0
        assertThat(latestNumBytesIn, Matchers.greaterThan(0L));
        // Add another split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaPartitionSplit(new TopicPartition(TOPIC2, 0), 0L))));
        reader.fetch();
        // We just check if numBytesIn is increasing
        assertThat(numBytesInCounter.getCount(), Matchers.greaterThan(latestNumBytesIn));
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(strings = {"_underscore.period-minus"})
    public void testPendingRecordsGauge(String topicSuffix) throws Throwable {
        final String topic1Name = TOPIC1 + topicSuffix;
        final String topic2Name = TOPIC2 + topicSuffix;
        if (!topicSuffix.isEmpty()) {
            KafkaSourceTestEnv.setupTopic(
                    topic1Name, true, true, KafkaSourceTestEnv::getRecordsForTopic);
            KafkaSourceTestEnv.setupTopic(
                    topic2Name, true, true, KafkaSourceTestEnv::getRecordsForTopic);
        }
        MetricListener metricListener = new MetricListener();
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        KafkaPartitionSplitReader reader =
                createReader(
                        props,
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        // Add a split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaPartitionSplit(new TopicPartition(topic1Name, 0), 0L))));
        // pendingRecords should have not been registered because of lazily registration
        assertFalse(metricListener.getGauge(MetricNames.PENDING_RECORDS).isPresent());
        // Trigger first fetch
        reader.fetch();
        final Optional<Gauge<Long>> pendingRecords =
                metricListener.getGauge(MetricNames.PENDING_RECORDS);
        assertTrue(pendingRecords.isPresent());
        // Validate pendingRecords
        assertNotNull(pendingRecords);
        assertEquals(NUM_RECORDS_PER_PARTITION - 1, (long) pendingRecords.get().getValue());
        for (int i = 1; i < NUM_RECORDS_PER_PARTITION; i++) {
            reader.fetch();
            assertEquals(NUM_RECORDS_PER_PARTITION - i - 1, (long) pendingRecords.get().getValue());
        }
        // Add another split
        reader.handleSplitsChanges(
                new SplitsAddition<>(
                        Collections.singletonList(
                                new KafkaPartitionSplit(new TopicPartition(topic2Name, 0), 0L))));
        // Validate pendingRecords
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            reader.fetch();
            assertEquals(NUM_RECORDS_PER_PARTITION - i - 1, (long) pendingRecords.get().getValue());
        }
    }

    @Test
    public void testAssignEmptySplit() throws Exception {
        KafkaPartitionSplitReader reader = createReader();
        final KafkaPartitionSplit normalSplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC1, 0),
                        KafkaPartitionSplit.EARLIEST_OFFSET,
                        KafkaPartitionSplit.NO_STOPPING_OFFSET);
        final KafkaPartitionSplit emptySplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC2, 0),
                        KafkaPartitionSplit.LATEST_OFFSET,
                        KafkaPartitionSplit.LATEST_OFFSET);
        reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(normalSplit, emptySplit)));

        // Fetch and check empty splits is added to finished splits
        RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> recordsWithSplitIds = reader.fetch();
        assertTrue(recordsWithSplitIds.finishedSplits().contains(emptySplit.splitId()));

        // Assign another valid split to avoid consumer.poll() blocking
        final KafkaPartitionSplit anotherNormalSplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC1, 1),
                        KafkaPartitionSplit.EARLIEST_OFFSET,
                        KafkaPartitionSplit.NO_STOPPING_OFFSET);
        reader.handleSplitsChanges(
                new SplitsAddition<>(Collections.singletonList(anotherNormalSplit)));

        // Fetch again and check empty split set is cleared
        recordsWithSplitIds = reader.fetch();
        assertTrue(recordsWithSplitIds.finishedSplits().isEmpty());
    }

    // ------------------

    private void assignSplitsAndFetchUntilFinish(KafkaPartitionSplitReader reader, int readerId)
            throws IOException {
        Map<String, KafkaPartitionSplit> splits =
                assignSplits(reader, splitsByOwners.get(readerId));

        Map<String, Integer> numConsumedRecords = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>();
        while (finishedSplits.size() < splits.size()) {
            RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> recordsBySplitIds = reader.fetch();
            String splitId = recordsBySplitIds.nextSplit();
            while (splitId != null) {
                // Collect the records in this split.
                List<ConsumerRecord<byte[], byte[]>> splitFetch = new ArrayList<>();
                ConsumerRecord<byte[], byte[]> record;
                while ((record = recordsBySplitIds.nextRecordFromSplit()) != null) {
                    splitFetch.add(record);
                }

                // Compute the expected next offset for the split.
                TopicPartition tp = splits.get(splitId).getTopicPartition();
                long earliestOffset = earliestOffsets.get(tp);
                int numConsumedRecordsForSplit = numConsumedRecords.getOrDefault(splitId, 0);
                long expectedStartingOffset = earliestOffset + numConsumedRecordsForSplit;

                // verify the consumed records.
                if (verifyConsumed(splits.get(splitId), expectedStartingOffset, splitFetch)) {
                    finishedSplits.add(splitId);
                }
                numConsumedRecords.compute(
                        splitId,
                        (ignored, recordCount) ->
                                recordCount == null
                                        ? splitFetch.size()
                                        : recordCount + splitFetch.size());
                splitId = recordsBySplitIds.nextSplit();
            }
        }

        // Verify the number of records consumed from each split.
        numConsumedRecords.forEach(
                (splitId, recordCount) -> {
                    TopicPartition tp = splits.get(splitId).getTopicPartition();
                    long earliestOffset = earliestOffsets.get(tp);
                    long expectedRecordCount = NUM_RECORDS_PER_PARTITION - earliestOffset;
                    assertEquals(
                            expectedRecordCount,
                            (long) recordCount,
                            String.format(
                                    "%s should have %d records.",
                                    splits.get(splitId), expectedRecordCount));
                });
    }

    // ------------------

    private KafkaPartitionSplitReader createReader() {
        return createReader(
                new Properties(), UnregisteredMetricsGroup.createSourceReaderMetricGroup());
    }

    private KafkaPartitionSplitReader createReader(
            Properties additionalProperties, SourceReaderMetricGroup sourceReaderMetricGroup) {
        Properties props = new Properties();
        props.putAll(KafkaSourceTestEnv.getConsumerProperties(ByteArrayDeserializer.class));
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        if (!additionalProperties.isEmpty()) {
            props.putAll(additionalProperties);
        }
        KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(sourceReaderMetricGroup);
        return new KafkaPartitionSplitReader(
                props,
                new TestingReaderContext(new Configuration(), sourceReaderMetricGroup),
                kafkaSourceReaderMetrics);
    }

    private Map<String, KafkaPartitionSplit> assignSplits(
            KafkaPartitionSplitReader reader, Map<String, KafkaPartitionSplit> splits) {
        SplitsChange<KafkaPartitionSplit> splitsChange =
                new SplitsAddition<>(new ArrayList<>(splits.values()));
        reader.handleSplitsChanges(splitsChange);
        return splits;
    }

    private boolean verifyConsumed(
            final KafkaPartitionSplit split,
            final long expectedStartingOffset,
            final Collection<ConsumerRecord<byte[], byte[]>> consumed) {
        long expectedOffset = expectedStartingOffset;

        for (ConsumerRecord<byte[], byte[]> record : consumed) {
            int expectedValue = (int) expectedOffset;
            long expectedTimestamp = expectedOffset * 1000L;

            assertEquals(expectedValue, deserializer.deserialize(record.topic(), record.value()));
            assertEquals(expectedOffset, record.offset());
            assertEquals(expectedTimestamp, record.timestamp());

            expectedOffset++;
        }
        if (split.getStoppingOffset().isPresent()) {
            return expectedOffset == split.getStoppingOffset().get();
        } else {
            return false;
        }
    }
}
