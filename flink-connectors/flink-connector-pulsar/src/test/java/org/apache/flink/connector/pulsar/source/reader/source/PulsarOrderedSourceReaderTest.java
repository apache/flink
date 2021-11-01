package org.apache.flink.connector.pulsar.source.reader.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.reader.PulsarSourceReaderFactory;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchemaInitializationContext;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.testutils.source.reader.SourceReaderTestBase;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;
import org.apache.flink.connector.testutils.source.reader.TestingReaderOutput;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;
import org.apache.flink.core.io.InputStatus;

import org.apache.flink.core.testutils.CommonTestUtils;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;



@ExtendWith(TestLoggerExtension.class)
class PulsarOrderedSourceReaderTest extends SourceReaderTestBase<PulsarPartitionSplit> {

    private static final String TOPIC = "PulsarOrderedSourceReaderTest";

    @RegisterExtension
    static final PulsarTestEnvironment environment = new PulsarTestEnvironment(runtime());

    /**
     * Choose the desired pulsar runtime as the test backend. The default test backend is a mocked
     * pulsar broker. Override this method when needs.
     */
    private static PulsarRuntime runtime() {
        return PulsarRuntime.mock();
    }

    /** Operate pulsar by acquiring a runtime operator. */
    private static PulsarRuntimeOperator operator() {
        return environment.operator();
    }

    @BeforeEach
    void beforeAll() {
        Random random = new Random(System.currentTimeMillis());
        operator().setupTopic(TOPIC, Schema.INT32, () -> random.nextInt(20), NUM_RECORDS_PER_SPLIT);
    }

    @AfterEach
    void afterAll() {
        operator().deleteTopic(TOPIC, true);
    }

    @Test
    public void testCommitOffsets() throws Exception {
        try (PulsarOrderedSourceReader<Integer> reader =
                     (PulsarOrderedSourceReader<Integer>)
                             createReader()) {
            // we use only one partition
            PulsarPartitionSplit split = createPartitionSplit(TOPIC, 0);
            reader.addSplits(Collections.singletonList(split));
            reader.notifyNoMoreSplits();
            TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
            InputStatus status;
            // we poll until we get all expected records. Otherwise if we start check
            // status != nputStatus.NOTHING_AVAILABLE this could be true upon entry : there
            // is a time when AddSplits task is dequeued from task queue, and assigned splits is
            // still empty (because AddSplits task has not begun execution), and this will
            // return NOTHING_AVAILABLE
            pollUntil(
                    reader,
                    output,
                    () -> output.getEmittedRecords().size() == NUM_RECORDS_PER_SPLIT,
                    "The output didn't poll enough records before timeout.");
            do {
                status = reader.pollNext(output);
            } while (status != InputStatus.NOTHING_AVAILABLE);
            reader.snapshotState(100L);
            reader.notifyCheckpointComplete(100L);
            pollUntil(
                    reader,
                    output,
                    () -> reader.getCursorsToCommit().isEmpty(),
                    "The offset commit did not finish before timeout.");

            TopicStats topicStats = operator().admin().topics()
                    .getStats(TopicNameUtils.topicNameWithPartition(TOPIC, 0),
                            true,
                            true);
            // verify if the messages has been consumed
            Map<String, ? extends SubscriptionStats> subscriptionStats = topicStats.getSubscriptions();
            Assertions.assertEquals(NUM_RECORDS_PER_SPLIT, topicStats.getMsgInCounter());
            Assertions.assertEquals(1, subscriptionStats.size());
            subscriptionStats.forEach(
                    (subscription, stats) -> {
                        Assertions.assertEquals(0, stats.getUnackedMessages());
                        Assertions.assertEquals(NUM_RECORDS_PER_SPLIT, stats.getMsgOutCounter());
                    });
        }
    }

    @Test
    public void testAssignZeroSplitsCreatesZeroSubscription() throws Exception {
        try (PulsarOrderedSourceReader<Integer> reader =
                     (PulsarOrderedSourceReader<Integer>)
                             createReader()) {
            reader.snapshotState(100L);
            reader.notifyCheckpointComplete(100L);
        }
        // Verify the committed offsets.
        for (int i = 0; i < PulsarRuntimeOperator.DEFAULT_PARTITIONS; i++) {
            Map<String, ? extends SubscriptionStats> subscriptionStats =
                    operator().admin().topics()
                            .getStats(TopicNameUtils.topicNameWithPartition(TOPIC, i), true, true)
                            .getSubscriptions();
            Assertions.assertEquals(0, subscriptionStats.size());
            subscriptionStats.forEach(
                    (subscription, stats) -> {
                        Assertions.assertEquals(0, stats.getUnackedMessages());
                    });
        }
    }

    @Test
    public void testOffsetCommitOnCheckpointComplete() throws Exception {
        try (PulsarOrderedSourceReader<Integer> reader =
                     (PulsarOrderedSourceReader<Integer>)
                             createReader()) {
            reader.addSplits(
                    getSplits(numSplits, NUM_RECORDS_PER_SPLIT, Boundedness.CONTINUOUS_UNBOUNDED));
            ValidatingSourceOutput output = new ValidatingSourceOutput();
            long checkpointId = 0;
            do {
                checkpointId++;
                reader.pollNext(output);
                // Create a checkpoint for each message consumption, but not complete them.
                reader.snapshotState(checkpointId);
            } while (output.count() < totalNumRecords);

            // The completion of the last checkpoint should subsume all previous checkpoints.
            Assertions.assertEquals(checkpointId, reader.getCursorsToCommit().size());

            long lastCheckpointId = checkpointId;
            CommonTestUtils.waitUtil(
                    () -> {
                        try {
                            reader.notifyCheckpointComplete(lastCheckpointId);
                        } catch (Exception exception) {
                            throw new RuntimeException(
                                    "Caught unexpected exception when polling from the reader",
                                    exception);
                        }
                        return reader.getCursorsToCommit().isEmpty();
                    },
                    Duration.ofSeconds(60),
                    Duration.ofSeconds(1),
                    "The offset commit did not finish before timeout.");
        }

        // Verify the committed offsets.
        for (int i = 0; i < PulsarRuntimeOperator.DEFAULT_PARTITIONS; i++) {
            Map<String, ? extends SubscriptionStats> subscriptionStats =
                    operator().admin().topics()
                            .getStats(TopicNameUtils.topicNameWithPartition(TOPIC, i), true, true)
                            .getSubscriptions();
            Assertions.assertEquals(1, subscriptionStats.size());
            subscriptionStats.forEach(
                    (subscription, stats) -> {
                        Assertions.assertEquals(0, stats.getUnackedMessages());
                        Assertions.assertEquals(NUM_RECORDS_PER_SPLIT, stats.getMsgOutCounter());
                    });
        }
    }

    @Test
    public void testNotCommitOffsetsForUninitializedSplits() throws Exception {
        final long checkpointId = 1234L;
        try (PulsarOrderedSourceReader<Integer> reader = (PulsarOrderedSourceReader<Integer>) createReader()) {
            PulsarPartitionSplit split = createPartitionSplit(TOPIC, 0);
            reader.addSplits(Collections.singletonList(split));
            reader.snapshotState(checkpointId);
            Assertions.assertEquals(1, reader.getCursorsToCommit().size());
            Assertions.assertTrue(reader.getCursorsToCommit().get(checkpointId).isEmpty());
        }
    }

    @Test
    public void testAssigningEmptySplits() throws Exception {
        final PulsarPartitionSplit emptySplit = createPartitionSplit(
                TOPIC,
                0,
                StopCursor.latest(),
                MessageId.latest);

        try (final PulsarOrderedSourceReader<Integer> reader =
                     (PulsarOrderedSourceReader<Integer>)
                             createReader()) {
            reader.addSplits(Collections.singletonList(emptySplit));

            TestingReaderOutput<Integer> output = new TestingReaderOutput<>();
            InputStatus status;
            status = reader.pollNext(output);
            // We sleep for 1 sec to avoid entering false negative case:
            // we perform assertion when add splits task
            // is dequeued but not executed.
            Thread.sleep(1000);
            Assertions.assertEquals(status, InputStatus.NOTHING_AVAILABLE);
        }
    }

    // ---------------------
    private void pollUntil(
            PulsarOrderedSourceReader<Integer> reader,
            ReaderOutput<Integer> output,
            Supplier<Boolean> condition,
            String errorMessage)
            throws Exception {
        CommonTestUtils.waitUtil(
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

    private SourceReader<Integer, PulsarPartitionSplit> createReader(
            Boundedness boundedness) throws Exception {
        return createReader(boundedness, new TestingReaderContext());
    }

    private SourceReader<Integer, PulsarPartitionSplit> createReader(
            Boundedness boundedness,
            SourceReaderContext context)
            throws Exception {

        PulsarDeserializationSchema<Integer> deserializationSchema =
                PulsarDeserializationSchema.pulsarSchema(Schema.INT32);
        deserializationSchema.open(
                new PulsarDeserializationSchemaInitializationContext(context));
        Configuration configuration = operator().config();
        configuration.set(PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME,
                "PulsarOrderedSourceReaderTestSubscription");
        configuration.set(PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.Failover);
        SourceConfiguration sourceConfiguration = new SourceConfiguration(configuration);
        return PulsarSourceReaderFactory.create(context, deserializationSchema, configuration,
                sourceConfiguration);
    }

    /**
     * This is used by Exclusive, Failover mode to create partition splits. It uses fullRange().
     * In further test cases these methods will be refactored to support more subscription types.
     * @param topic
     * @param partitionId
     * @return
     */
    private PulsarPartitionSplit createPartitionSplit(String topic, int partitionId) {
        return createPartitionSplit(topic, partitionId, StopCursor.defaultStopCursor());
    }

    private PulsarPartitionSplit createPartitionSplit(String topic, int partitionId,
                                                      StopCursor stopCursor) {
        return createPartitionSplit(topic, partitionId, stopCursor, MessageId.earliest);
    }

    private PulsarPartitionSplit createPartitionSplit(String topic,
                                                      int partitionId,
                                                      StopCursor stopCursor,
                                                      MessageId latestConsumedId) {
        TopicPartition topicPartition =  new TopicPartition(topic, partitionId,
                TopicRange.createFullRange());
        return new PulsarPartitionSplit(
                topicPartition,
                stopCursor,
                latestConsumedId,
                null);
    }

    /**
     * By default, the reader doesn't distinguish between bounededness. Boundedness affects
     * enumerator only. Readers will use whatever stop cursor the split has to determine the
     * default behaviour.
     * @return
     * @throws Exception
     */
    @Override
    protected SourceReader<Integer, PulsarPartitionSplit> createReader() throws Exception {
        return createReader(Boundedness.CONTINUOUS_UNBOUNDED, new TestingReaderContext());
    }

    @Override
    protected List<PulsarPartitionSplit> getSplits(
            int numSplits, int numRecordsPerSplit, Boundedness boundedness) {
        List<PulsarPartitionSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(getSplit(i, numRecordsPerSplit, boundedness));
        }
        return splits;
    }

    @Override
    protected PulsarPartitionSplit getSplit(int splitId, int numRecords, Boundedness boundedness) {
        // note: when we used latest() stop cursor, there is a bug in test case :subscription
        // outMessageCounter will be 20 (twice as NUM_RECORDS_PER_SPLIT)
        StopCursor stopCursor =
                boundedness == Boundedness.BOUNDED
                        ? StopCursor.latest()
                        : StopCursor.never();
        return createPartitionSplit(TOPIC, splitId, stopCursor);
    }


    /**
     *      First,The base test class implementation is tightly restricted by Kafka implementations
     * To fully utilize it, we need to design a mapping from pulsar message id to a
     * natural number sequence. Currently we'll set the value to NUM_RECORDS_PER_SPLIT
     * to compile the test cases. Note this NUM_RECORDS_PER_SPLIT is not necessarily
     * the same as PulsarRuntimeOperator.NUM_RECORDS_PER_PARTITION.
     *      Second, base class is using JUnit 4, thus this class can't use the base class for now.
     * @param split
     * @return
     */
    @Override
    protected long getNextRecordIndex(PulsarPartitionSplit split) {
        return NUM_RECORDS_PER_SPLIT;
    }

    @Override
    protected int getNumSplits() {
        return PulsarRuntimeOperator.DEFAULT_PARTITIONS;
    }
}

