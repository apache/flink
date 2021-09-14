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

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.KafkaSourceTestEnv;
import org.apache.flink.connector.kafka.source.KafkaSourceTestUtils;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.COMMITS_SUCCEEDED_METRIC_COUNTER;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.COMMITTED_OFFSET_METRIC_GAUGE;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.CURRENT_OFFSET_METRIC_GAUGE;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.INITIAL_OFFSET;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.KAFKA_CONSUMER_METRIC_GROUP;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.PARTITION_GROUP;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.TOPIC_GROUP;
import static org.apache.flink.core.testutils.CommonTestUtils.waitUtil;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link KafkaSourceReader}. */
public class KafkaSourceReaderTest extends SourceReaderTestBase<KafkaPartitionSplit> {
    private static final String TOPIC = "KafkaSourceReaderTest";

    @BeforeClass
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
            adminClient.createTopics(
                    Collections.singleton(new NewTopic(TOPIC, NUM_SPLITS, (short) 1)));
            // Use the admin client to trigger the creation of internal __consumer_offsets topic.
            // This makes sure that we won't see unavailable coordinator in the tests.
            waitUtil(
                    () -> {
                        try {
                            adminClient
                                    .listConsumerGroupOffsets("AnyGroup")
                                    .partitionsToOffsetAndMetadata()
                                    .get();
                        } catch (Exception e) {
                            return false;
                        }
                        return true;
                    },
                    Duration.ofSeconds(60),
                    "Waiting for offsets topic creation failed.");
        }
        KafkaSourceTestEnv.produceToKafka(
                getRecords(), StringSerializer.class, IntegerSerializer.class);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    // -----------------------------------------

    @Test
    public void testCommitOffsetsWithoutAliveFetchers() throws Exception {
        final String groupId = "testCommitOffsetsWithoutAliveFetchers";
        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(Boundedness.CONTINUOUS_UNBOUNDED, groupId)) {
            KafkaPartitionSplit split =
                    new KafkaPartitionSplit(new TopicPartition(TOPIC, 0), 0, NUM_RECORDS_PER_SPLIT);
            reader.addSplits(Collections.singletonList(split));
            reader.notifyNoMoreSplits();
            ReaderOutput<Integer> output = new TestingReaderOutput<>();
            InputStatus status;
            do {
                status = reader.pollNext(output);
            } while (status != InputStatus.NOTHING_AVAILABLE);
            pollUntil(
                    reader,
                    output,
                    () -> reader.getNumAliveFetchers() == 0,
                    "The split fetcher did not exit before timeout.");
            reader.snapshotState(100L);
            reader.notifyCheckpointComplete(100L);
            // Due to a bug in KafkaConsumer, when the consumer closes, the offset commit callback
            // won't be fired, so the offsetsToCommit map won't be cleaned. To make the test
            // stable, we add a split whose starting offset is the log end offset, so the
            // split fetcher won't become idle and exit after commitOffsetAsync is invoked from
            // notifyCheckpointComplete().
            reader.addSplits(
                    Collections.singletonList(
                            new KafkaPartitionSplit(
                                    new TopicPartition(TOPIC, 0), NUM_RECORDS_PER_SPLIT)));
            pollUntil(
                    reader,
                    output,
                    () -> reader.getOffsetsToCommit().isEmpty(),
                    "The offset commit did not finish before timeout.");
        }
        // Verify the committed offsets.
        try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
            assertEquals(1, committedOffsets.size());
            committedOffsets.forEach(
                    (tp, offsetAndMetadata) ->
                            assertEquals(NUM_RECORDS_PER_SPLIT, offsetAndMetadata.offset()));
        }
    }

    @Test
    public void testCommitEmptyOffsets() throws Exception {
        final String groupId = "testCommitEmptyOffsets";
        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(Boundedness.CONTINUOUS_UNBOUNDED, groupId)) {
            reader.snapshotState(100L);
            reader.notifyCheckpointComplete(100L);
        }
        // Verify the committed offsets.
        try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
            assertTrue(committedOffsets.isEmpty());
        }
    }

    @Test
    public void testOffsetCommitOnCheckpointComplete() throws Exception {
        final String groupId = "testOffsetCommitOnCheckpointComplete";
        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(Boundedness.CONTINUOUS_UNBOUNDED, groupId)) {
            reader.addSplits(
                    getSplits(NUM_SPLITS, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED));
            ValidatingSourceOutput output = new ValidatingSourceOutput();
            long checkpointId = 0;
            do {
                checkpointId++;
                reader.pollNext(output);
                // Create a checkpoint for each message consumption, but not complete them.
                reader.snapshotState(checkpointId);
            } while (output.count() < TOTAL_NUM_RECORDS);

            // The completion of the last checkpoint should subsume all the previous checkpoitns.
            assertEquals(checkpointId, reader.getOffsetsToCommit().size());
            reader.notifyCheckpointComplete(checkpointId);
            pollUntil(
                    reader,
                    output,
                    () -> reader.getOffsetsToCommit().isEmpty(),
                    "The offset commit did not finish before timeout.");
        }

        // Verify the committed offsets.
        try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
            Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                    adminClient
                            .listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata()
                            .get();
            assertEquals(NUM_SPLITS, committedOffsets.size());
            committedOffsets.forEach(
                    (tp, offsetAndMetadata) ->
                            assertEquals(NUM_RECORDS_PER_SPLIT, offsetAndMetadata.offset()));
        }
    }

    @Test
    public void testNotCommitOffsetsForUninitializedSplits() throws Exception {
        final long checkpointId = 1234L;
        try (KafkaSourceReader<Integer> reader = (KafkaSourceReader<Integer>) createReader()) {
            KafkaPartitionSplit split =
                    new KafkaPartitionSplit(
                            new TopicPartition(TOPIC, 0), KafkaPartitionSplit.EARLIEST_OFFSET);
            reader.addSplits(Collections.singletonList(split));
            reader.snapshotState(checkpointId);
            assertEquals(1, reader.getOffsetsToCommit().size());
            assertTrue(reader.getOffsetsToCommit().get(checkpointId).isEmpty());
        }
    }

    @Test
    public void testDisableOffsetCommit() throws Exception {
        final Properties properties = new Properties();
        properties.setProperty(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "false");
        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                new TestingReaderContext(),
                                (ignore) -> {},
                                properties)) {
            reader.addSplits(
                    getSplits(NUM_SPLITS, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED));
            ValidatingSourceOutput output = new ValidatingSourceOutput();
            long checkpointId = 0;
            do {
                checkpointId++;
                reader.pollNext(output);
                // Create a checkpoint for each message consumption, but not complete them.
                reader.snapshotState(checkpointId);
                // Offsets to commit should be always empty because offset commit is disabled
                assertEquals(0, reader.getOffsetsToCommit().size());
            } while (output.count() < TOTAL_NUM_RECORDS);
        }
    }

    @Test
    public void testKafkaSourceMetrics() throws Exception {
        final MetricListener metricListener = new MetricListener();
        final String groupId = "testKafkaSourceMetrics";
        final TopicPartition tp0 = new TopicPartition(TOPIC, 0);
        final TopicPartition tp1 = new TopicPartition(TOPIC, 1);

        try (KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.CONTINUOUS_UNBOUNDED,
                                groupId,
                                metricListener.getMetricGroup())) {

            KafkaPartitionSplit split0 =
                    new KafkaPartitionSplit(tp0, KafkaPartitionSplit.EARLIEST_OFFSET);
            KafkaPartitionSplit split1 =
                    new KafkaPartitionSplit(tp1, KafkaPartitionSplit.EARLIEST_OFFSET);
            reader.addSplits(Arrays.asList(split0, split1));

            TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
            pollUntil(
                    reader,
                    output,
                    () -> output.getEmittedRecords().size() == NUM_RECORDS_PER_SPLIT * 2,
                    String.format(
                            "Failed to poll %d records until timeout", NUM_RECORDS_PER_SPLIT * 2));

            // Metric "records-consumed-total" of KafkaConsumer should be NUM_RECORDS_PER_SPLIT
            assertEquals(
                    NUM_RECORDS_PER_SPLIT * 2,
                    getKafkaConsumerMetric("records-consumed-total", metricListener));

            // Current consuming offset should be NUM_RECORD_PER_SPLIT - 1
            assertEquals(NUM_RECORDS_PER_SPLIT - 1, getCurrentOffsetMetric(tp0, metricListener));
            assertEquals(NUM_RECORDS_PER_SPLIT - 1, getCurrentOffsetMetric(tp1, metricListener));

            // No offset is committed till now
            assertEquals(INITIAL_OFFSET, getCommittedOffsetMetric(tp0, metricListener));
            assertEquals(INITIAL_OFFSET, getCommittedOffsetMetric(tp1, metricListener));

            // Trigger offset commit
            reader.snapshotState(15213L);
            reader.notifyCheckpointComplete(15213L);
            waitUtil(
                    () -> reader.getOffsetsToCommit().isEmpty(),
                    Duration.ofSeconds(60),
                    String.format(
                            "Offsets are not committed successfully. Dangling offsets: %s",
                            reader.getOffsetsToCommit()));

            // Metric "commit-total" of KafkaConsumer should be 1
            assertEquals(1, getKafkaConsumerMetric("commit-total", metricListener));

            // Committed offset should be NUM_RECORD_PER_SPLIT
            assertEquals(NUM_RECORDS_PER_SPLIT, getCommittedOffsetMetric(tp0, metricListener));
            assertEquals(NUM_RECORDS_PER_SPLIT, getCommittedOffsetMetric(tp1, metricListener));

            // Number of successful commits should be 1
            assertEquals(
                    1L,
                    metricListener
                            .getCounter(
                                    KAFKA_SOURCE_READER_METRIC_GROUP,
                                    COMMITS_SUCCEEDED_METRIC_COUNTER)
                            .getCount());
        }
    }

    @Test
    public void testAssigningEmptySplits() throws Exception {
        // Normal split with NUM_RECORDS_PER_SPLIT records
        final KafkaPartitionSplit normalSplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC, 0), 0, KafkaPartitionSplit.LATEST_OFFSET);
        // Empty split with no record
        final KafkaPartitionSplit emptySplit =
                new KafkaPartitionSplit(
                        new TopicPartition(TOPIC, 1), NUM_RECORDS_PER_SPLIT, NUM_RECORDS_PER_SPLIT);
        // Split finished hook for listening finished splits
        final Set<String> finishedSplits = new HashSet<>();
        final Consumer<Collection<String>> splitFinishedHook = finishedSplits::addAll;

        try (final KafkaSourceReader<Integer> reader =
                (KafkaSourceReader<Integer>)
                        createReader(
                                Boundedness.BOUNDED,
                                "KafkaSourceReaderTestGroup",
                                new TestingReaderContext(),
                                splitFinishedHook)) {
            reader.addSplits(Arrays.asList(normalSplit, emptySplit));
            pollUntil(
                    reader,
                    new TestingReaderOutput<>(),
                    () -> reader.getNumAliveFetchers() == 0,
                    "The split fetcher did not exit before timeout.");
            MatcherAssert.assertThat(
                    finishedSplits,
                    Matchers.containsInAnyOrder(
                            KafkaPartitionSplit.toSplitId(normalSplit.getTopicPartition()),
                            KafkaPartitionSplit.toSplitId(emptySplit.getTopicPartition())));
        }
    }

    // ------------------------------------------

    @Override
    protected SourceReader<Integer, KafkaPartitionSplit> createReader() throws Exception {
        return createReader(Boundedness.BOUNDED, "KafkaSourceReaderTestGroup");
    }

    @Override
    protected List<KafkaPartitionSplit> getSplits(
            int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
        List<KafkaPartitionSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(getSplit(i, numRecordsPerSplit, boundedness));
        }
        return splits;
    }

    @Override
    protected KafkaPartitionSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
        long stoppingOffset =
                boundedness == Boundedness.BOUNDED
                        ? NUM_RECORDS_PER_SPLIT
                        : KafkaPartitionSplit.NO_STOPPING_OFFSET;
        return new KafkaPartitionSplit(new TopicPartition(TOPIC, splitId), 0L, stoppingOffset);
    }

    @Override
    protected long getNextRecordIndex(KafkaPartitionSplit split) {
        return split.getStartingOffset();
    }

    // ---------------------

    private SourceReader<Integer, KafkaPartitionSplit> createReader(
            Boundedness boundedness, String groupId) throws Exception {
        return createReader(boundedness, groupId, new TestingReaderContext(), (ignore) -> {});
    }

    private SourceReader<Integer, KafkaPartitionSplit> createReader(
            Boundedness boundedness, String groupId, MetricGroup metricGroup) throws Exception {
        return createReader(
                boundedness,
                groupId,
                new TestingReaderContext(new Configuration(), metricGroup),
                (ignore) -> {});
    }

    private SourceReader<Integer, KafkaPartitionSplit> createReader(
            Boundedness boundedness,
            String groupId,
            SourceReaderContext context,
            Consumer<Collection<String>> splitFinishedHook)
            throws Exception {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return createReader(boundedness, context, splitFinishedHook, properties);
    }

    private SourceReader<Integer, KafkaPartitionSplit> createReader(
            Boundedness boundedness,
            SourceReaderContext context,
            Consumer<Collection<String>> splitFinishedHook,
            Properties props)
            throws Exception {
        KafkaSourceBuilder<Integer> builder =
                KafkaSource.<Integer>builder()
                        .setClientIdPrefix("KafkaSourceReaderTest")
                        .setDeserializer(
                                KafkaRecordDeserializationSchema.valueOnly(
                                        IntegerDeserializer.class))
                        .setPartitions(Collections.singleton(new TopicPartition("AnyTopic", 0)))
                        .setProperty(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                KafkaSourceTestEnv.brokerConnectionStrings)
                        .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                        .setProperties(props);
        if (boundedness == Boundedness.BOUNDED) {
            builder.setBounded(OffsetsInitializer.latest());
        }

        return KafkaSourceTestUtils.createReaderWithFinishedSplitHook(
                builder.build(), context, splitFinishedHook);
    }

    private void pollUntil(
            KafkaSourceReader<Integer> reader,
            ReaderOutput<Integer> output,
            Supplier<Boolean> condition,
            String errorMessage)
            throws Exception {
        waitUtil(
                () -> {
                    try {
                        reader.pollNext(output);
                    } catch (Exception exception) {
                        throw new RuntimeException(
                                "Caught unexpected exception when polling from the reader",
                                exception);
                    }
                    return condition.get();
                },
                Duration.ofSeconds(60),
                errorMessage);
    }

    private long getKafkaConsumerMetric(String name, MetricListener listener) {
        return ((Double)
                        listener.getGauge(
                                        KAFKA_SOURCE_READER_METRIC_GROUP,
                                        KAFKA_CONSUMER_METRIC_GROUP,
                                        name)
                                .getValue())
                .longValue();
    }

    private long getCurrentOffsetMetric(TopicPartition tp, MetricListener listener) {
        return (long)
                listener.getGauge(
                                KAFKA_SOURCE_READER_METRIC_GROUP,
                                TOPIC_GROUP,
                                tp.topic(),
                                PARTITION_GROUP,
                                String.valueOf(tp.partition()),
                                CURRENT_OFFSET_METRIC_GAUGE)
                        .getValue();
    }

    private long getCommittedOffsetMetric(TopicPartition tp, MetricListener listener) {
        return (long)
                listener.getGauge(
                                KAFKA_SOURCE_READER_METRIC_GROUP,
                                TOPIC_GROUP,
                                tp.topic(),
                                PARTITION_GROUP,
                                String.valueOf(tp.partition()),
                                COMMITTED_OFFSET_METRIC_GAUGE)
                        .getValue();
    }

    // ---------------------

    private static List<ProducerRecord<String, Integer>> getRecords() {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();
        for (int part = 0; part < NUM_SPLITS; part++) {
            for (int i = 0; i < NUM_RECORDS_PER_SPLIT; i++) {
                records.add(
                        new ProducerRecord<>(
                                TOPIC, part, TOPIC + "-" + part, part * NUM_RECORDS_PER_SPLIT + i));
            }
        }
        return records;
    }
}
