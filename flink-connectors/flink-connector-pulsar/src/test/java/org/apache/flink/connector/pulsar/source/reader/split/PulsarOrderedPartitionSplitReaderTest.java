package org.apache.flink.connector.pulsar.source.reader.split;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchemaInitializationContext;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.NUM_RECORDS_PER_PARTITION;

class PulsarOrderedPartitionSplitReaderTest extends PulsarTestSuiteBase {

    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";

    private static final int READER0 = 0;
    private static final int READER1 = 1;

    private static Map<Integer, Map<String, PulsarPartitionSplit>> splitsByOwners;

    @BeforeAll
    void beforeAll() {
        AtomicInteger atomicInteger1 = new AtomicInteger(0);
        AtomicInteger atomicInteger2 = new AtomicInteger(0);
        operator().setupTopic(TOPIC1, Schema.INT32, atomicInteger1::getAndIncrement,
                NUM_RECORDS_PER_PARTITION);
        operator().setupTopic(TOPIC2, Schema.INT32, atomicInteger2::getAndIncrement,
                NUM_RECORDS_PER_PARTITION);
        splitsByOwners = new HashMap<>();
        splitsByOwners.put(READER0, predefineSplitsByOwners(READER0, TOPIC1));
        splitsByOwners.put(READER1, predefineSplitsByOwners(READER1, TOPIC2));
    }

    @AfterAll
    void afterAll() {
        operator().deleteTopic(TOPIC1, true);
        operator().deleteTopic(TOPIC2, true);
    }

    @Test
    public void testHandleSplitChangesAndFetch() throws Exception {
        PulsarOrderedPartitionSplitReader<Integer> reader0 = createSplitReader();
        assignSplitsAndFetchUntilFinish(reader0, READER0);
        reader0.close();
    }

    @Test
    public void testWakeUp() throws Exception {
        PulsarOrderedPartitionSplitReader<Integer> reader = createSplitReader();
        PulsarPartitionSplit nonExistingTopicPartition = createPartitionSplit("non-exist", 0);
        assignSplits(
                reader,
                Collections.singletonMap(
                        nonExistingTopicPartition.splitId(),
                        nonExistingTopicPartition));
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
        Assertions.assertNull(error.get());
    }

    @Test
    public void testAssignEmptySplit() throws Exception {
        PulsarOrderedPartitionSplitReader<Integer> reader = createSplitReader();
        final PulsarPartitionSplit emptySplit = createPartitionSplit(TOPIC2,
                0,
                StopCursor.latest(),
                MessageId.latest);
        reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(emptySplit)));

        // Fetch and check empty splits is added to finished splits
        RecordsWithSplitIds<PulsarMessage<Integer>> recordsWithSplitIds = reader.fetch();
        Assertions.assertTrue(recordsWithSplitIds.finishedSplits().contains(emptySplit.splitId()));
        reader.close();
    }

    // ------------------

    private void assignSplitsAndFetchUntilFinish(
            PulsarOrderedPartitionSplitReader<Integer> reader, int readerId) throws IOException {
        Map<String, PulsarPartitionSplit> splits =
                assignSplits(reader, splitsByOwners.get(readerId));

        Map<String, Integer> numConsumedRecords = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>();
        while (finishedSplits.size() < splits.size()) {
            RecordsWithSplitIds<PulsarMessage<Integer>> recordsBySplitIds = reader.fetch();
            String splitId = recordsBySplitIds.nextSplit();
            while (splitId != null) {
                // Collect the records in this split.
                List<PulsarMessage<Integer>> splitFetch = new ArrayList<>();
                PulsarMessage<Integer> record;
                while ((record = recordsBySplitIds.nextRecordFromSplit()) != null) {
                    splitFetch.add(record);
                }

                // Compute the expected next offset for the split.
                TopicPartition tp = splits.get(splitId).getPartition();
                int numConsumedRecordsForSplit = numConsumedRecords.getOrDefault(splitId, 0);
                long expectedStartingOffset = numConsumedRecordsForSplit;

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
                    TopicPartition tp = splits.get(splitId).getPartition();
                    long expectedRecordCount = NUM_RECORDS_PER_PARTITION;
                    Assertions.assertEquals(
                            expectedRecordCount,
                            (long) recordCount,
                            String.format(
                                    "%s should have %d records.",
                                    splits.get(splitId), expectedRecordCount));
                });
    }

    // ------------------

    private PulsarOrderedPartitionSplitReader<Integer> createSplitReader() throws Exception {
        return createSplitReader(Boundedness.CONTINUOUS_UNBOUNDED, new TestingReaderContext());
    }

    private PulsarOrderedPartitionSplitReader<Integer> createSplitReader(
            Boundedness boundedness,
            SourceReaderContext context)
            throws Exception {

        PulsarDeserializationSchema<Integer> deserializationSchema =
                PulsarDeserializationSchema.pulsarSchema(Schema.INT32);

        deserializationSchema.open(
                new PulsarDeserializationSchemaInitializationContext(context));
        Configuration configuration = operator().config();
        configuration.set(
                PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME,
                "PulsarOrderedPartitionSplitReaderTestSubscription");
        configuration.set(PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.Failover);
        SourceConfiguration sourceConfiguration = new SourceConfiguration(configuration);

        PulsarClient client = PulsarConfigUtils.createClient(configuration);
        PulsarAdmin admin = PulsarConfigUtils.createAdmin(configuration);
        return new PulsarOrderedPartitionSplitReader<>(
                client,
                admin,
                configuration,
                sourceConfiguration,
                deserializationSchema);
    }

    private Map<String, PulsarPartitionSplit> assignSplits(
            PulsarOrderedPartitionSplitReader<Integer> reader, Map<String, PulsarPartitionSplit> splits) {
        SplitsChange<PulsarPartitionSplit> splitsChange =
                new SplitsAddition<>(new ArrayList<>(splits.values()));
        reader.handleSplitsChanges(splitsChange);
        return splits;
    }

    private boolean verifyConsumed(
            final PulsarPartitionSplit split,
            final long expectedStartingOffset,
            final Collection<PulsarMessage<Integer>> consumed) {
        long expectedOffset = expectedStartingOffset;

        for (PulsarMessage<Integer> record : consumed) {
            int expectedValue = (int) expectedOffset;
            Assertions.assertEquals(expectedValue, (int) record.getValue());
            expectedOffset++;
        }
        if (split.getLatestConsumedId() != null) {
            // our split is boundedness, so we use a predefined number of records per split
            // to mark the end of the test
            return expectedOffset == NUM_RECORDS_PER_PARTITION;
        } else {
            return false;
        }
    }

    /**
     * Currently one SplitReader is responsible for one split. We don't need to have a
     * complex interleaved assignment.
     * @param reader
     * @param topic
     * @return
     */
    private Map<String, PulsarPartitionSplit> predefineSplitsByOwners(int reader, String topic) {
        final Map<String, PulsarPartitionSplit> splits = new HashMap<>();
        PulsarPartitionSplit newSplit = createPartitionSplit(topic, 0);
        splits.put(newSplit.splitId(), newSplit);
        return splits;
    }

    private PulsarPartitionSplit createPartitionSplit(String topic, int partitionId) {
        return createPartitionSplit(topic, partitionId, StopCursor.defaultStopCursor());
    }

    private PulsarPartitionSplit createPartitionSplit(String topic, int partitionId,
                                                      StopCursor stopCursor) {
        TopicPartition topicPartition =  new TopicPartition(topic, partitionId,
                TopicRange.createFullRange());
        return new PulsarPartitionSplit(
                topicPartition,
                stopCursor,
                MessageId.earliest,
                null);
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
}
