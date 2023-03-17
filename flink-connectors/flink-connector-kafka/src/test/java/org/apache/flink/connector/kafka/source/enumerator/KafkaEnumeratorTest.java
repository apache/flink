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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.kafka.source.KafkaSourceOptions;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;
import org.apache.flink.mock.Whitebox;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link KafkaSourceEnumerator}. */
public class KafkaEnumeratorTest {
    private static final int NUM_SUBTASKS = 3;
    private static final String DYNAMIC_TOPIC_NAME = "dynamic_topic";
    private static final int NUM_PARTITIONS_DYNAMIC_TOPIC = 4;

    private static final String TOPIC1 = "topic";
    private static final String TOPIC2 = "pattern-topic";

    private static final int READER0 = 0;
    private static final int READER1 = 1;
    private static final Set<String> PRE_EXISTING_TOPICS =
            new HashSet<>(Arrays.asList(TOPIC1, TOPIC2));
    private static final int PARTITION_DISCOVERY_CALLABLE_INDEX = 0;
    private static final boolean ENABLE_PERIODIC_PARTITION_DISCOVERY = true;
    private static final boolean DISABLE_PERIODIC_PARTITION_DISCOVERY = false;
    private static final boolean INCLUDE_DYNAMIC_TOPIC = true;
    private static final boolean EXCLUDE_DYNAMIC_TOPIC = false;

    @BeforeClass
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.setupTopic(TOPIC1, true, true, KafkaSourceTestEnv::getRecordsForTopic);
        KafkaSourceTestEnv.setupTopic(TOPIC2, true, true, KafkaSourceTestEnv::getRecordsForTopic);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        KafkaSourceTestEnv.tearDown();
    }

    @Test
    public void testStartWithDiscoverPartitionsOnce() throws Exception {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getPeriodicCallables()).isEmpty();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);

            // enumerator just start noMoreNewPartitionSplits will be false
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isFalse();
        }
    }

    @Test
    public void testStartWithPeriodicPartitionDiscovery() throws Exception {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getOneTimeCallables()).isEmpty();
            assertThat(context.getPeriodicCallables())
                    .as("A periodic partition discovery callable should have been scheduled")
                    .hasSize(1);

            // enumerator just start noMoreNewPartitionSplits will be false
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isFalse();
        }
    }

    @Test
    public void testDiscoverPartitionsTriggersAssignments() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();

            // register reader 0.
            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);

            // Verify assignments for reader 0.
            verifyLastReadersAssignments(
                    context, Arrays.asList(READER0, READER1), PRE_EXISTING_TOPICS, 1);
        }
    }

    @Test
    public void testReaderRegistrationTriggersAssignments() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            runOneTimePartitionDiscovery(context);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);

            registerReader(context, enumerator, READER1);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 2);
        }
    }

    @Test
    public void testRunWithDiscoverPartitionsOnceToCheckNoMoreSplit() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
            assertThat(context.getPeriodicCallables()).isEmpty();
            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);

            // enumerator noMoreNewPartitionSplits first will be false, when execute
            // handlePartitionSplitChanges will be set true
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isTrue();
        }
    }

    @Test
    public void testRunWithPeriodicPartitionDiscoveryOnceToCheckNoMoreSplit() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getOneTimeCallables()).isEmpty();
            assertThat(context.getPeriodicCallables())
                    .as("A periodic partition discovery callable should have been scheduled")
                    .hasSize(1);
            // Run the partition discover callable and check the partition assignment.
            runPeriodicPartitionDiscovery(context);

            // enumerator noMoreNewPartitionSplits first will be false, even when execute
            // handlePartitionSplitChanges it still be false
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isFalse();
        }
    }

    @Test
    public void testRunWithDiscoverPartitionsOnceWithZeroMsToCheckNoMoreSplit() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                // set partitionDiscoveryIntervalMs = 0
                KafkaSourceEnumerator enumerator = createEnumerator(context, 0L)) {

            // Start the enumerator, and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
            assertThat(context.getPeriodicCallables()).isEmpty();
            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);

            // enumerator noMoreNewPartitionSplits first will be false, when execute
            // handlePartitionSplitChanges will be set true
            assertThat((Boolean) Whitebox.getInternalState(enumerator, "noMoreNewPartitionSplits"))
                    .isTrue();
        }
    }

    @Test(timeout = 30000L)
    public void testDiscoverPartitionsPeriodically() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY,
                                INCLUDE_DYNAMIC_TOPIC);
                AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {

            startEnumeratorAndRegisterReaders(context, enumerator);

            // invoke partition discovery callable again and there should be no new assignments.
            runPeriodicPartitionDiscovery(context);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("No assignments should be made because there is no partition change")
                    .hasSize(2);

            // create the dynamic topic.
            adminClient
                    .createTopics(
                            Collections.singleton(
                                    new NewTopic(
                                            DYNAMIC_TOPIC_NAME,
                                            NUM_PARTITIONS_DYNAMIC_TOPIC,
                                            (short) 1)))
                    .all()
                    .get();

            // invoke partition discovery callable again.
            while (true) {
                runPeriodicPartitionDiscovery(context);
                if (context.getSplitsAssignmentSequence().size() < 3) {
                    Thread.sleep(10);
                } else {
                    break;
                }
            }
            verifyLastReadersAssignments(
                    context,
                    Arrays.asList(READER0, READER1),
                    Collections.singleton(DYNAMIC_TOPIC_NAME),
                    3);
        } finally {
            try (AdminClient adminClient = KafkaSourceTestEnv.getAdminClient()) {
                adminClient.deleteTopics(Collections.singleton(DYNAMIC_TOPIC_NAME)).all().get();
            } catch (Exception e) {
                // Let it go.
            }
        }
    }

    @Test
    public void testAddSplitsBack() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            startEnumeratorAndRegisterReaders(context, enumerator);

            // Simulate a reader failure.
            context.unregisterReader(READER0);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0),
                    READER0);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("The added back splits should have not been assigned")
                    .hasSize(2);

            // Simulate a reader recovery.
            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 3);
        }
    }

    @Test
    public void testWorkWithPreexistingAssignments() throws Throwable {
        Set<TopicPartition> preexistingAssignments;
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context1 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context1, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {
            startEnumeratorAndRegisterReaders(context1, enumerator);
            preexistingAssignments =
                    asEnumState(context1.getSplitsAssignmentSequence().get(0).assignment());
        }

        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context2 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context2,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY ? 1 : -1,
                                PRE_EXISTING_TOPICS,
                                preexistingAssignments,
                                new Properties())) {
            enumerator.start();
            runPeriodicPartitionDiscovery(context2);

            registerReader(context2, enumerator, READER0);
            assertThat(context2.getSplitsAssignmentSequence()).isEmpty();

            registerReader(context2, enumerator, READER1);
            verifyLastReadersAssignments(
                    context2, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 1);
        }
    }

    @Test
    public void testKafkaClientProperties() throws Exception {
        Properties properties = new Properties();
        String clientIdPrefix = "test-prefix";
        Integer defaultTimeoutMs = 99999;
        properties.setProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), clientIdPrefix);
        properties.setProperty(
                ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(defaultTimeoutMs));
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY ? 1 : -1,
                                PRE_EXISTING_TOPICS,
                                Collections.emptySet(),
                                properties)) {
            enumerator.start();

            AdminClient adminClient =
                    (AdminClient) Whitebox.getInternalState(enumerator, "adminClient");
            assertThat(adminClient).isNotNull();
            String clientId = (String) Whitebox.getInternalState(adminClient, "clientId");
            assertThat(clientId).isNotNull().startsWith(clientIdPrefix);
            assertThat(Whitebox.getInternalState(adminClient, "defaultApiTimeoutMs"))
                    .isEqualTo(defaultTimeoutMs);

            assertThat(clientId).isNotNull().startsWith(clientIdPrefix);
        }
    }

    @Test
    public void testSnapshotState() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator = createEnumerator(context, false)) {
            enumerator.start();

            // No reader is registered, so the state should be empty
            final KafkaSourceEnumState state1 = enumerator.snapshotState(1L);
            assertThat(state1.assignedPartitions()).isEmpty();

            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            runOneTimePartitionDiscovery(context);

            // The state should contain splits assigned to READER0 and READER1
            final KafkaSourceEnumState state2 = enumerator.snapshotState(1L);
            verifySplitAssignmentWithPartitions(
                    getExpectedAssignments(
                            new HashSet<>(Arrays.asList(READER0, READER1)), PRE_EXISTING_TOPICS),
                    state2.assignedPartitions());
        }
    }

    @Test
    public void testPartitionChangeChecking() throws Throwable {
        try (MockSplitEnumeratorContext<KafkaPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                KafkaSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {
            enumerator.start();
            runOneTimePartitionDiscovery(context);
            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);

            // All partitions of TOPIC1 and TOPIC2 should have been discovered now

            // Check partition change using only DYNAMIC_TOPIC_NAME-0
            TopicPartition newPartition = new TopicPartition(DYNAMIC_TOPIC_NAME, 0);
            Set<TopicPartition> fetchedPartitions = new HashSet<>();
            fetchedPartitions.add(newPartition);
            final KafkaSourceEnumerator.PartitionChange partitionChange =
                    enumerator.getPartitionChange(fetchedPartitions);

            // Since enumerator never met DYNAMIC_TOPIC_NAME-0, it should be mark as a new partition
            Set<TopicPartition> expectedNewPartitions = Collections.singleton(newPartition);

            // All existing topics are not in the fetchedPartitions, so they should be marked as
            // removed
            Set<TopicPartition> expectedRemovedPartitions = new HashSet<>();
            for (int i = 0; i < KafkaSourceTestEnv.NUM_PARTITIONS; i++) {
                expectedRemovedPartitions.add(new TopicPartition(TOPIC1, i));
                expectedRemovedPartitions.add(new TopicPartition(TOPIC2, i));
            }

            assertThat(partitionChange.getNewPartitions()).isEqualTo(expectedNewPartitions);
            assertThat(partitionChange.getRemovedPartitions()).isEqualTo(expectedRemovedPartitions);
        }
    }

    // -------------- some common startup sequence ---------------

    private void startEnumeratorAndRegisterReaders(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context,
            KafkaSourceEnumerator enumerator)
            throws Throwable {
        // Start the enumerator and it should schedule a one time task to discover and assign
        // partitions.
        enumerator.start();

        // register reader 0 before the partition discovery.
        registerReader(context, enumerator, READER0);
        assertThat(context.getSplitsAssignmentSequence()).isEmpty();

        // Run the partition discover callable and check the partition assignment.
        runPeriodicPartitionDiscovery(context);
        verifyLastReadersAssignments(
                context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);

        // Register reader 1 after first partition discovery.
        registerReader(context, enumerator, READER1);
        verifyLastReadersAssignments(
                context, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 2);
    }

    // ----------------------------------------

    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery) {
        return createEnumerator(
                enumContext, enablePeriodicPartitionDiscovery, EXCLUDE_DYNAMIC_TOPIC);
    }

    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            long partitionDiscoveryIntervalMs) {
        return createEnumerator(enumContext, partitionDiscoveryIntervalMs, EXCLUDE_DYNAMIC_TOPIC);
    }

    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            boolean includeDynamicTopic) {
        List<String> topics = new ArrayList<>(PRE_EXISTING_TOPICS);
        if (includeDynamicTopic) {
            topics.add(DYNAMIC_TOPIC_NAME);
        }
        return createEnumerator(
                enumContext,
                enablePeriodicPartitionDiscovery ? 1 : -1,
                topics,
                Collections.emptySet(),
                new Properties());
    }

    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            long partitionDiscoveryIntervalMs,
            boolean includeDynamicTopic) {
        List<String> topics = new ArrayList<>(PRE_EXISTING_TOPICS);
        if (includeDynamicTopic) {
            topics.add(DYNAMIC_TOPIC_NAME);
        }
        return createEnumerator(
                enumContext,
                partitionDiscoveryIntervalMs,
                topics,
                Collections.emptySet(),
                new Properties());
    }

    /**
     * Create the enumerator. For the purpose of the tests in this class we don't care about the
     * subscriber and offsets initializer, so just use arbitrary settings.
     */
    private KafkaSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<KafkaPartitionSplit> enumContext,
            long partitionDiscoveryIntervalMs,
            Collection<String> topicsToSubscribe,
            Set<TopicPartition> assignedPartitions,
            Properties overrideProperties) {
        // Use a TopicPatternSubscriber so that no exception if a subscribed topic hasn't been
        // created yet.
        StringJoiner topicNameJoiner = new StringJoiner("|");
        topicsToSubscribe.forEach(topicNameJoiner::add);
        Pattern topicPattern = Pattern.compile(topicNameJoiner.toString());
        KafkaSubscriber subscriber = KafkaSubscriber.getTopicPatternSubscriber(topicPattern);

        OffsetsInitializer startingOffsetsInitializer = OffsetsInitializer.earliest();
        OffsetsInitializer stoppingOffsetsInitializer = new NoStoppingOffsetsInitializer();

        Properties props =
                new Properties(KafkaSourceTestEnv.getConsumerProperties(StringDeserializer.class));
        KafkaSourceEnumerator.deepCopyProperties(overrideProperties, props);
        props.setProperty(
                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
                String.valueOf(partitionDiscoveryIntervalMs));

        return new KafkaSourceEnumerator(
                subscriber,
                startingOffsetsInitializer,
                stoppingOffsetsInitializer,
                props,
                enumContext,
                Boundedness.CONTINUOUS_UNBOUNDED,
                assignedPartitions);
    }

    // ---------------------

    private void registerReader(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context,
            KafkaSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "location 0"));
        enumerator.addReader(reader);
    }

    private void verifyLastReadersAssignments(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context,
            Collection<Integer> readers,
            Set<String> topics,
            int expectedAssignmentSeqSize) {
        verifyAssignments(
                getExpectedAssignments(new HashSet<>(readers), topics),
                context.getSplitsAssignmentSequence()
                        .get(expectedAssignmentSeqSize - 1)
                        .assignment());
    }

    private void verifyAssignments(
            Map<Integer, Set<TopicPartition>> expectedAssignments,
            Map<Integer, List<KafkaPartitionSplit>> actualAssignments) {
        actualAssignments.forEach(
                (reader, splits) -> {
                    Set<TopicPartition> expectedAssignmentsForReader =
                            expectedAssignments.get(reader);
                    assertThat(expectedAssignmentsForReader).isNotNull();
                    assertThat(splits.size()).isEqualTo(expectedAssignmentsForReader.size());
                    for (KafkaPartitionSplit split : splits) {
                        assertThat(expectedAssignmentsForReader)
                                .contains(split.getTopicPartition());
                    }
                });
    }

    private Map<Integer, Set<TopicPartition>> getExpectedAssignments(
            Set<Integer> readers, Set<String> topics) {
        Map<Integer, Set<TopicPartition>> expectedAssignments = new HashMap<>();
        Set<TopicPartition> allPartitions = new HashSet<>();

        if (topics.contains(DYNAMIC_TOPIC_NAME)) {
            for (int i = 0; i < NUM_PARTITIONS_DYNAMIC_TOPIC; i++) {
                allPartitions.add(new TopicPartition(DYNAMIC_TOPIC_NAME, i));
            }
        }

        for (TopicPartition tp : KafkaSourceTestEnv.getPartitionsForTopics(PRE_EXISTING_TOPICS)) {
            if (topics.contains(tp.topic())) {
                allPartitions.add(tp);
            }
        }

        for (TopicPartition tp : allPartitions) {
            int ownerReader = KafkaSourceEnumerator.getSplitOwner(tp, NUM_SUBTASKS);
            if (readers.contains(ownerReader)) {
                expectedAssignments.computeIfAbsent(ownerReader, r -> new HashSet<>()).add(tp);
            }
        }
        return expectedAssignments;
    }

    private void verifySplitAssignmentWithPartitions(
            Map<Integer, Set<TopicPartition>> expectedAssignment,
            Set<TopicPartition> actualTopicPartitions) {
        final Set<TopicPartition> allTopicPartitionsFromAssignment = new HashSet<>();
        expectedAssignment.forEach(
                (reader, topicPartitions) ->
                        allTopicPartitionsFromAssignment.addAll(topicPartitions));
        assertThat(actualTopicPartitions).isEqualTo(allTopicPartitionsFromAssignment);
    }

    private Set<TopicPartition> asEnumState(Map<Integer, List<KafkaPartitionSplit>> assignments) {
        Set<TopicPartition> enumState = new HashSet<>();
        assignments.forEach(
                (reader, assignment) ->
                        assignment.forEach(split -> enumState.add(split.getTopicPartition())));
        return enumState;
    }

    private void runOneTimePartitionDiscovery(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context) throws Throwable {
        // Fetch potential topic descriptions
        context.runNextOneTimeCallable();
        // Initialize offsets for discovered partitions
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }

    private void runPeriodicPartitionDiscovery(
            MockSplitEnumeratorContext<KafkaPartitionSplit> context) throws Throwable {
        // Fetch potential topic descriptions
        context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
        // Initialize offsets for discovered partitions
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }
}
