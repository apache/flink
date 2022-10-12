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
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState.initialState;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicPatternSubscriber;
import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator.DEFAULT_PARTITIONS;
import static org.apache.flink.shaded.guava30.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PulsarSourceEnumerator}. */
class PulsarSourceEnumeratorTest extends PulsarTestSuiteBase {

    private static final int NUM_SUBTASKS = 3;
    private static final int READER0 = 0;
    private static final int READER1 = 1;
    private static final int READER2 = 2;
    private static final int PARTITION_DISCOVERY_CALLABLE_INDEX = 0;
    private static final boolean ENABLE_PERIODIC_PARTITION_DISCOVERY = true;
    private static final boolean DISABLE_PERIODIC_PARTITION_DISCOVERY = false;

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void startWithDiscoverPartitionsOnce(SubscriptionType subscriptionType) throws Exception {
        Set<String> preexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                preexistingTopics,
                                context,
                                DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            // Start the enumerator and it should schedule a one time task to discover and assign
            // partitions.
            enumerator.start();
            assertThat(context.getPeriodicCallables()).isEmpty();
            assertThat(context.getOneTimeCallables())
                    .as("A one time partition discovery callable should have been scheduled")
                    .hasSize(1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void startWithPeriodicPartitionDiscovery(SubscriptionType subscriptionType) throws Exception {
        Set<String> preexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                preexistingTopics,
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            enumerator.start();
            assertThat(context.getOneTimeCallables()).isEmpty();
            assertThat((context.getPeriodicCallables()))
                    .as("A periodic partition discovery callable should have been scheduled")
                    .hasSize(1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void discoverPartitionsTriggersAssignments(SubscriptionType subscriptionType) throws Throwable {
        Set<String> preexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                preexistingTopics,
                                context,
                                DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            enumerator.start();

            // register reader 0, 1, 2
            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            registerReader(context, enumerator, READER2);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);
            verifyAllReaderAssignments(subscriptionType, context, preexistingTopics, 1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void discoverPartitionsPeriodically(SubscriptionType subscriptionType) throws Throwable {
        String dynamicTopic = "topic3-" + randomAlphabetic(10);
        Set<String> preexistingTopics = setupPreexistingTopics();
        Set<String> topicsToSubscribe = new HashSet<>(preexistingTopics);
        topicsToSubscribe.add(dynamicTopic);
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                topicsToSubscribe,
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            testRegisterReadersForPreexistingTopics(
                    subscriptionType, preexistingTopics, context, enumerator);

            // invoke partition discovery callable again and there should be no new assignments.
            runPeriodicPartitionDiscovery(context);

            assertThat(context.getSplitsAssignmentSequence())
                    .as("No new assignments should be made because there is no partition change")
                    .hasSize(3);

            // Create the dynamic topic.
            operator().createTopic(dynamicTopic, DEFAULT_PARTITIONS);

            // Invoke partition discovery callable again.
            while (true) {
                runPeriodicPartitionDiscovery(context);
                if (context.getSplitsAssignmentSequence().size() < 4) {
                    sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                } else {
                    break;
                }
            }
            verifyAllReaderAssignments(subscriptionType, context, topicsToSubscribe, 4);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void addSplitsBack(SubscriptionType subscriptionType) throws Throwable {
        Set<String> preexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                preexistingTopics,
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            testRegisterReadersForPreexistingTopics(
                    subscriptionType, preexistingTopics, context, enumerator);

            // Simulate a reader failure.
            context.unregisterReader(READER0);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0),
                    READER0);
            assertThat(context.getSplitsAssignmentSequence())
                    .as("The added back splits should have not been assigned")
                    .hasSize(3);

            // Simulate a reader recovery.
            registerReader(context, enumerator, READER0);
            verifyAllReaderAssignments(subscriptionType, context, preexistingTopics, 3 + 1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void workWithPreexistingAssignments(SubscriptionType subscriptionType) throws Throwable {
        Set<String> preexistingTopics = setupPreexistingTopics();
        PulsarSourceEnumState preexistingAssignments;
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context1 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                preexistingTopics,
                                context1,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY)) {
            testRegisterReadersForPreexistingTopics(
                    subscriptionType, preexistingTopics, context1, enumerator);
            preexistingAssignments =
                    asEnumState(context1.getSplitsAssignmentSequence().get(0).assignment());
        }

        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context2 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                preexistingTopics,
                                context2,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY,
                                preexistingAssignments)) {
            enumerator.start();
            runPeriodicPartitionDiscovery(context2);

            registerReader(context2, enumerator, READER0);
            if (subscriptionType == SubscriptionType.Shared) {
                verifyAllReaderAssignments(subscriptionType, context2, preexistingTopics, 1);
            } else {
                assertThat(context2.getSplitsAssignmentSequence()).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void snapshotState(SubscriptionType subscriptionType) throws Throwable {
        Set<String> preexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(subscriptionType, preexistingTopics, context, false)) {
            enumerator.start();

            // No reader is registered, so the state should be empty
            final PulsarSourceEnumState state1 = enumerator.snapshotState(1L);
            assertThat(state1.getAppendedPartitions()).isEmpty();

            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            runOneTimePartitionDiscovery(context);

            // The state should contain splits assigned to READER0 and READER1
            final PulsarSourceEnumState state2 = enumerator.snapshotState(1L);
            verifySplitAssignmentWithPartitions(
                    getExpectedTopicPartitions(preexistingTopics), state2.getAppendedPartitions());
        }
    }

    private Set<String> setupPreexistingTopics() {
        String topic1 = "topic1-" + randomAlphabetic(10);
        String topic2 = "topic2-" + randomAlphabetic(10);

        operator().setupTopic(topic1);
        operator().setupTopic(topic2);

        return ImmutableSet.of(topic1, topic2);
    }

    private void testRegisterReadersForPreexistingTopics(
            SubscriptionType subscriptionType,
            Set<String> topics,
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumerator enumerator)
            throws Throwable {
        enumerator.start();

        // register reader 0 before the partition discovery.
        registerReader(context, enumerator, READER0);
        assertThat(context.getSplitsAssignmentSequence()).isEmpty();

        // Run the partition discover callable and check the partition assignment.
        runPeriodicPartitionDiscovery(context);
        if (subscriptionType == SubscriptionType.Shared) {
            verifyAllReaderAssignments(subscriptionType, context, topics, 1);
        }

        registerReader(context, enumerator, READER1);
        registerReader(context, enumerator, READER2);

        verifyAllReaderAssignments(subscriptionType, context, topics, 3);
    }

    private PulsarSourceEnumerator createEnumerator(
            SubscriptionType subscriptionType,
            Set<String> topics,
            MockSplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery) {
        return createEnumerator(
                subscriptionType,
                topics,
                enumContext,
                enablePeriodicPartitionDiscovery,
                initialState());
    }

    private PulsarSourceEnumerator createEnumerator(
            SubscriptionType subscriptionType,
            Set<String> topicsToSubscribe,
            MockSplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery,
            PulsarSourceEnumState sourceEnumState) {
        // Use a TopicPatternSubscriber so that no exception if a subscribed topic hasn't been
        // created yet.
        String topicRegex = String.join("|", topicsToSubscribe);
        Pattern topicPattern = Pattern.compile(topicRegex);
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(topicPattern, RegexSubscriptionMode.AllTopics);

        Configuration configuration = operator().config();
        configuration.set(PULSAR_SUBSCRIPTION_TYPE, subscriptionType);
        configuration.set(PULSAR_SUBSCRIPTION_NAME, randomAlphabetic(10));
        if (enablePeriodicPartitionDiscovery) {
            configuration.set(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 60L);
        } else {
            configuration.set(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, -1L);
        }

        return new PulsarSourceEnumerator(
                subscriber,
                StartCursor.earliest(),
                StopCursor.latest(),
                new FullRangeGenerator(),
                new SourceConfiguration(configuration),
                enumContext,
                sourceEnumState);
    }

    private void registerReader(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "testing location"));
        enumerator.addReader(reader);
    }

    private void verifyAllReaderAssignments(
            SubscriptionType subscriptionType,
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            Set<String> topics,
            int expectedAssignmentSeqSize) {
        assertThat(context.getSplitsAssignmentSequence()).hasSize(expectedAssignmentSeqSize);
        // Merge the assignments into one
        List<SplitsAssignment<PulsarPartitionSplit>> sequence =
                context.getSplitsAssignmentSequence();
        Map<Integer, Set<PulsarPartitionSplit>> assignments = new HashMap<>();

        for (int i = 0; i < expectedAssignmentSeqSize; i++) {
            Map<Integer, List<PulsarPartitionSplit>> assignment = sequence.get(i).assignment();
            assignment.forEach(
                    (key, value) ->
                            assignments.computeIfAbsent(key, k -> new HashSet<>()).addAll(value));
        }

        // Compare assigned partitions with desired partitions.
        Set<TopicPartition> expectedTopicPartitions = getExpectedTopicPartitions(topics);
        if (subscriptionType == SubscriptionType.Failover) {
            int actualSize = assignments.values().stream().mapToInt(Set::size).sum();
            assertThat(actualSize).isEqualTo(expectedTopicPartitions.size());
        } else if (subscriptionType == SubscriptionType.Shared) {
            assignments
                    .values()
                    .forEach(
                            (splits) -> assertThat(splits).hasSize(expectedTopicPartitions.size()));
        }
    }

    private Set<TopicPartition> getExpectedTopicPartitions(Set<String> topics) {
        Set<TopicPartition> allPartitions = new HashSet<>();
        for (String topicName : topics) {
            for (int i = 0; i < DEFAULT_PARTITIONS; i++) {
                allPartitions.add(new TopicPartition(topicName, i));
            }
        }
        return allPartitions;
    }

    private void verifySplitAssignmentWithPartitions(
            Set<TopicPartition> expectedAssignment, Set<TopicPartition> actualTopicPartitions) {
        assertThat(actualTopicPartitions).isEqualTo(expectedAssignment);
    }

    // this method only works for non-Shared Mode
    private PulsarSourceEnumState asEnumState(
            Map<Integer, List<PulsarPartitionSplit>> assignments) {
        Set<TopicPartition> appendedPartitions =
                assignments.values().stream()
                        .flatMap(List::stream)
                        .map(PulsarPartitionSplit::getPartition)
                        .collect(toSet());

        return new PulsarSourceEnumState(appendedPartitions);
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
