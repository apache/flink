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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;

import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor.latest;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicPatternSubscriber;

/** Unit tests for {@link PulsarSourceEnumerator}. */
class PulsarSourceEnumeratorTest extends PulsarTestSuiteBase {

    private static final int NUM_SUBTASKS = 3;
    private static final String DYNAMIC_TOPIC_NAME = "dynamic_topic";
    private static final String TOPIC1 = "topic";
    private static final String TOPIC2 = "pattern-topic";
    private static final int READER0 = 0;
    private static final int READER1 = 1;
    private static final Set<String> PRE_EXISTING_TOPICS = Sets.newHashSet(TOPIC1, TOPIC2);
    private static final int PARTITION_DISCOVERY_CALLABLE_INDEX = 0;
    private static final boolean ENABLE_PERIODIC_PARTITION_DISCOVERY = true;
    private static final boolean DISABLE_PERIODIC_PARTITION_DISCOVERY = false;
    private static final boolean INCLUDE_DYNAMIC_TOPIC = true;
    private static final boolean EXCLUDE_DYNAMIC_TOPIC = false;

    // @TestInstance(TestInstance.Lifecycle.PER_CLASS) is annotated in PulsarTestSuitBase, so this
    // method could be non-static.
    @BeforeAll
    void beforeAll() {
        operator().setupTopic(TOPIC1);
        operator().setupTopic(TOPIC2);
    }

    @AfterAll
    void afterAll() {
        operator().deleteTopic(TOPIC1, true);
        operator().deleteTopic(TOPIC2, true);
    }

    @Test
    void startWithDiscoverPartitionsOnce() throws Exception {
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            Assertions.assertTrue(context.getPeriodicCallables().isEmpty());
            Assertions.assertEquals(
                    1,
                    context.getOneTimeCallables().size(),
                    "A one time partition discovery callable should have been scheduled");
        }
    }

    @Test
    void startWithPeriodicPartitionDiscovery() throws Exception {
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            Assertions.assertTrue(context.getOneTimeCallables().isEmpty());
            Assertions.assertEquals(
                    1,
                    context.getPeriodicCallables().size(),
                    "A periodic partition discovery callable should have been scheduled");
        }
    }

    @Test
    public void testDiscoverPartitionsTriggersAssignments() throws Throwable {
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                     new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             PulsarSourceEnumerator enumerator =
                     createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();

            // register reader 0, 1
            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            Assertions.assertTrue(context.getSplitsAssignmentSequence().isEmpty());

            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);

            // Notice that we don't validate individual assignments for read 0, 1, as read split
            // assignment relies on internal implementation. We pass the list of two readers only
            // as placeholder
            verifyLastReadersAssignments(
                    enumerator, context, Arrays.asList(READER0, READER1), PRE_EXISTING_TOPICS, 1);
        }
    }

    @Test
    public void testReaderRegistrationTriggersAssignments() throws Throwable {
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                     new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             PulsarSourceEnumerator enumerator =
                     createEnumerator(context, DISABLE_PERIODIC_PARTITION_DISCOVERY)) {
            enumerator.start();
            runOneTimePartitionDiscovery(context);
            Assertions.assertTrue(context.getSplitsAssignmentSequence().isEmpty());

            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    enumerator, context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);

            registerReader(context, enumerator, READER1);
            verifyLastReadersAssignments(
                    enumerator, context, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 1);
        }
    }

    @Test
    public void testDiscoverPartitionsPeriodically() throws Throwable {
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                     new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             PulsarSourceEnumerator enumerator =
                     createEnumerator(
                             context,
                             ENABLE_PERIODIC_PARTITION_DISCOVERY,
                             INCLUDE_DYNAMIC_TOPIC)) {

            startEnumeratorAndRegisterReaders(context, enumerator);

            // invoke partition discovery callable again and there should be no new assignments.
            runPeriodicPartitionDiscovery(context);
            Assertions.assertEquals(
                    1,
                    context.getSplitsAssignmentSequence().size(),
                    "No assignments should be made because there is no partition change");

            // create the dynamic topic.
            operator().createTopic(DYNAMIC_TOPIC_NAME, PulsarRuntimeOperator.DEFAULT_PARTITIONS);

            // invoke partition discovery callable again.
            while (true) {
                runPeriodicPartitionDiscovery(context);
                if (context.getSplitsAssignmentSequence().size() < 2) {
                    Thread.sleep(10);
                } else {
                    break;
                }
            }
            verifyLastReadersAssignments(
                    enumerator,
                    context,
                    Arrays.asList(READER0, READER1),
                    Collections.singleton(DYNAMIC_TOPIC_NAME),
                    2);
        } finally {
            operator().deleteTopic(DYNAMIC_TOPIC_NAME, PulsarRuntimeOperator.DEFAULT_PARTITIONS > 0);
        }
    }

    @Test
    public void testAddSplitsBack() throws Throwable {
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                     new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             PulsarSourceEnumerator enumerator =
                     createEnumerator(context, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            startEnumeratorAndRegisterReaders(context, enumerator);

            // Simulate a reader failure.
            context.unregisterReader(READER0);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0),
                    READER0);
            Assertions.assertEquals(
                    1,
                    context.getSplitsAssignmentSequence().size(),
                    "The added back splits should have not been assigned");

            // Simulate a reader recovery.
            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    enumerator, context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 2);
        }
    }

    @Test
    public void testWorkWithPreexistingAssignments() throws Throwable {
        PulsarSourceEnumState preexistingAssignments;
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context1 =
                     new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             PulsarSourceEnumerator enumerator =
                     createEnumerator(context1, ENABLE_PERIODIC_PARTITION_DISCOVERY)) {
            startEnumeratorAndRegisterReaders(context1, enumerator);
            preexistingAssignments =
                    asEnumState(context1.getSplitsAssignmentSequence().get(0).assignment());
        }

        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context2 =
                     new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             PulsarSourceEnumerator enumerator =
                     createEnumerator(
                             context2,
                             ENABLE_PERIODIC_PARTITION_DISCOVERY,
                             false,
                             preexistingAssignments)) {
            enumerator.start();
            runPeriodicPartitionDiscovery(context2);

            registerReader(context2, enumerator, READER0);
            verifyLastReadersAssignments(
                    enumerator, context2, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);
        }
    }

    @Test
    public void testSnapshotState() throws Throwable {
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                     new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
             PulsarSourceEnumerator enumerator = createEnumerator(context, false)) {
            enumerator.start();

            // No reader is registered, so the state should be empty
            final PulsarSourceEnumState state1 = enumerator.snapshotState(1L);
            Assertions.assertTrue(state1.getAppendedPartitions().isEmpty());

            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            runOneTimePartitionDiscovery(context);

            // The state should contain splits assigned to READER0 and READER1
            final PulsarSourceEnumState state2 = enumerator.snapshotState(1L);
            verifySplitAssignmentWithPartitions(
                    getExpectedTopicPartitions(
                            new HashSet<>(Arrays.asList(READER0, READER1)), PRE_EXISTING_TOPICS),
                    state2.getAppendedPartitions());
        }
    }

    private void startEnumeratorAndRegisterReaders(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumerator enumerator)
            throws Throwable {
        enumerator.start();

        // register reader 0 before the partition discovery.
        registerReader(context, enumerator, READER0);
        Assertions.assertTrue(context.getSplitsAssignmentSequence().isEmpty());

        // Run the partition discover callable and check the partition assignment.
        runPeriodicPartitionDiscovery(context);
        verifyLastReadersAssignments(
                enumerator, context, Collections.singleton(READER0), PRE_EXISTING_TOPICS, 1);

        // Register reader 1 after first partition discovery. Reader 0 already has every splits
        // so expect reader 1 has no splits assigned
        registerReader(context, enumerator, READER1);
        verifyLastReadersAssignments(
                enumerator, context, Collections.singleton(READER1), PRE_EXISTING_TOPICS, 1);
    }

    private PulsarSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            boolean enablePeriodicPartitionDiscovery) {
        return createEnumerator(context, enablePeriodicPartitionDiscovery, EXCLUDE_DYNAMIC_TOPIC);
    }

    private PulsarSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            boolean includeDynamicTopic) {
        PulsarSourceEnumState sourceEnumState =
                new PulsarSourceEnumState(
                        Sets.newHashSet(),
                        Sets.newHashSet(),
                        Maps.newHashMap(),
                        Maps.newHashMap(),
                        false);
        return createEnumerator(
                enumContext,
                enablePeriodicPartitionDiscovery,
                includeDynamicTopic,
                sourceEnumState);
    }

    private PulsarSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            boolean includeDynamicTopic,
            PulsarSourceEnumState sourceEnumState) {
        List<String> topics = new ArrayList<>(PRE_EXISTING_TOPICS);
        if (includeDynamicTopic) {
            topics.add(DYNAMIC_TOPIC_NAME);
        }
        Configuration configuration = operator().config();
        configuration.set(PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.Failover);
        return createEnumerator(
                enumContext,
                enablePeriodicPartitionDiscovery,
                topics,
                sourceEnumState,
                configuration);
    }

    /**
     * Create the enumerator. For the purpose of the tests in this class we don't care about the
     * subscriber and offsets initializer, so just use arbitrary settings.
     */
    private PulsarSourceEnumerator createEnumerator(
            MockSplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            Collection<String> topicsToSubscribe,
            PulsarSourceEnumState sourceEnumState,
            Configuration configuration) {
        // Use a TopicPatternSubscriber so that no exception if a subscribed topic hasn't been
        // created yet.
        String topicRegex = String.join("|", topicsToSubscribe);
        Pattern topicPattern = Pattern.compile(topicRegex);
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(topicPattern, RegexSubscriptionMode.AllTopics);
        if (enablePeriodicPartitionDiscovery) {
            configuration.set(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 60L);
        } else {
            configuration.set(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, -1L);
        }
        SourceConfiguration sourceConfiguration = new SourceConfiguration(configuration);
        SplitsAssignmentState assignmentState =
                new SplitsAssignmentState(latest(), sourceConfiguration, sourceEnumState);

        return new PulsarSourceEnumerator(
                subscriber,
                StartCursor.earliest(),
                new FullRangeGenerator(),
                configuration,
                sourceConfiguration,
                enumContext,
                assignmentState);
    }

    private void registerReader(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "testing location "));
        enumerator.addReader(reader);
    }

    private void verifyLastReadersAssignments(
            PulsarSourceEnumerator enumerator,
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            Collection<Integer> readers,
            Set<String> topics,
            int expectedAssignmentSeqSize) {
        verifyAssignments(
                SubscriptionType.Failover,
                enumerator,
                getExpectedTopicPartitions(new HashSet<>(readers), topics),
                context.getSplitsAssignmentSequence()
                        .get(expectedAssignmentSeqSize - 1)
                        .assignment());
    }

    private void verifyAssignments(
            SubscriptionType subscriptionType,
            PulsarSourceEnumerator enumerator,
            Set<TopicPartition> expectedTopicPartitions,
            Map<Integer, List<PulsarPartitionSplit>> actualAssignments) {
        if (subscriptionType == SubscriptionType.Failover) {
            Set<TopicPartition> actualTopicPartitions =
                    actualAssignments.values().stream()
                            .flatMap(Collection::stream)
                            .map(PulsarPartitionSplit::getPartition)
                            .collect(Collectors.toSet());
            Assertions.assertEquals(expectedTopicPartitions.size(), actualTopicPartitions.size());
        } else if (subscriptionType == SubscriptionType.Shared) {
            actualAssignments.forEach(
                    (reader, splits) -> {
                        Assertions.assertEquals(expectedTopicPartitions.size(), splits.size());
                        for (PulsarPartitionSplit split : splits) {
                            Assertions.assertTrue(
                                    expectedTopicPartitions.contains(split.getPartition()));
                        }
                    }
            );
        }

    }


    /**
     * Because the number of splits is not a 1 to 1 mapping of total partitions of topics. For example,
     * when in Shared mode, each PulsarPartition maps to multiple PulsarPartitionSplit, as every reader
     * will subscribe to the same PulsarPartition. In Key-shared mode, each partition generates multiple
     * PulsarPartition with different TopicRange. Thus the expected assignments depends on the subscription
     * type under test
     * @param readers
     * @param topics
     * @return
     */
    private Set<TopicPartition> getExpectedTopicPartitions(
            Set<Integer> readers, Set<String> topics) {
        Map<Integer, Set<TopicPartition>> expectedAssignments = new HashMap<>();
        Set<TopicPartition> allPartitions = new HashSet<>();

        // for Shared mode, PulsarPartition is not a 1-to-1 mapping to PulsarPartitionSplit
        // for other 3 modes, each unique PulsarPartition corresponds to one PulsarPartitionSplit
        if (topics.contains(DYNAMIC_TOPIC_NAME)) {
            for (int i = 0; i < PulsarRuntimeOperator.DEFAULT_PARTITIONS; i++) {
                // assume we are not using keyShared nor Shared mode
                allPartitions.add(new TopicPartition(DYNAMIC_TOPIC_NAME, i, TopicRange.createFullRange()));
            }
        }

        for (String topicName : PRE_EXISTING_TOPICS) {
            if (topics.contains(topicName)) {
                for (int i = 0; i < PulsarRuntimeOperator.DEFAULT_PARTITIONS; i++) {
                    allPartitions.add(new TopicPartition(topicName, i, TopicRange.createFullRange()));
                }
            }
        }

        return allPartitions;
    }

    private void verifySplitAssignmentWithPartitions(
            Set<TopicPartition> expectedAssignment,
            Set<TopicPartition> actualTopicPartitions) {
        Assertions.assertEquals(expectedAssignment, actualTopicPartitions);
    }


    /**
     * Currently this only test non Shared subscription, as it assumes a one to one mapping
     * from TopicPartition to PulsarPartitionSplits.
     * @param assignments
     * @return
     */
    private PulsarSourceEnumState asEnumState(Map<Integer, List<PulsarPartitionSplit>> assignments) {
        Set<TopicPartition> appendedPartitions = new HashSet<>();
        Set<PulsarPartitionSplit> pendingPartitionSplits = new HashSet<>();
        Map<Integer, Set<PulsarPartitionSplit>> sharedPendingPartitionSplits = new HashMap<>();
        Map<Integer, Set<String>> readerAssignedSplits = new HashMap<>();
        boolean initialized = false;

        assignments.values().forEach( splits -> {
            appendedPartitions.addAll(splits.stream()
                    .map(PulsarPartitionSplit::getPartition)
                    .collect(Collectors.toList()));
            pendingPartitionSplits.addAll(splits);
        });

        PulsarSourceEnumState enumState = new PulsarSourceEnumState(
                appendedPartitions,
                pendingPartitionSplits,
                sharedPendingPartitionSplits,
                readerAssignedSplits,
                initialized
        );
        return enumState;
    }

    private void runOneTimePartitionDiscovery(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context) throws Throwable {
        // Fetch potential topic descriptions
        context.runNextOneTimeCallable();
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }

    private void runPeriodicPartitionDiscovery(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context) throws Throwable {
        // Fetch potential topic descriptions
        context.runPeriodicCallable(PARTITION_DISCOVERY_CALLABLE_INDEX);
        if (!context.getOneTimeCallables().isEmpty()) {
            context.runNextOneTimeCallable();
        }
    }
}
