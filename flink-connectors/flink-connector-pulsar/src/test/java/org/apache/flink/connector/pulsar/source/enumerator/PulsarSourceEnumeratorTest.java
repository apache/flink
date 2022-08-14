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
import org.apache.flink.connector.pulsar.source.enumerator.assigner.SplitAssigner;
import org.apache.flink.connector.pulsar.source.enumerator.assigner.SplitAssignerFactory;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor.latest;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicPatternSubscriber;
import static org.apache.flink.shaded.guava30.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PulsarSourceEnumerator}. */
class PulsarSourceEnumeratorTest extends PulsarTestSuiteBase {

    private static final int NUM_SUBTASKS = 3;
    private static final int READER0 = 0;
    private static final int READER1 = 1;
    private static final int PARTITION_DISCOVERY_CALLABLE_INDEX = 0;
    private static final boolean ENABLE_PERIODIC_PARTITION_DISCOVERY = true;
    private static final boolean DISABLE_PERIODIC_PARTITION_DISCOVERY = false;

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void startWithDiscoverPartitionsOnce(SubscriptionType subscriptionType) throws Exception {
        Set<String> prexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                prexistingTopics,
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
        Set<String> prexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                prexistingTopics,
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
        Set<String> prexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                prexistingTopics,
                                context,
                                DISABLE_PERIODIC_PARTITION_DISCOVERY)) {

            enumerator.start();

            // register reader 0, 1
            registerReader(context, enumerator, READER0);
            registerReader(context, enumerator, READER1);
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // Run the partition discover callable and check the partition assignment.
            runOneTimePartitionDiscovery(context);
            verifyLastReadersAssignments(subscriptionType, context, prexistingTopics, 1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void discoverPartitionsPeriodically(SubscriptionType subscriptionType) throws Throwable {
        String dynamicTopic = randomAlphabetic(10);
        Set<String> prexistingTopics = setupPreexistingTopics();
        Set<String> topicsToSubscribe = new HashSet<>(prexistingTopics);
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
                    subscriptionType, prexistingTopics, context, enumerator);

            // invoke partition discovery callable again and there should be no new assignments.
            runPeriodicPartitionDiscovery(context);

            int expectedSplitsAssignmentSequenceSize =
                    subscriptionType == SubscriptionType.Failover ? 1 : 2;

            assertThat(context.getSplitsAssignmentSequence())
                    .as("No new assignments should be made because there is no partition change")
                    .hasSize(expectedSplitsAssignmentSequenceSize);

            // create the dynamic topic.
            operator().createTopic(dynamicTopic, PulsarRuntimeOperator.DEFAULT_PARTITIONS);

            // invoke partition discovery callable again.
            while (true) {
                runPeriodicPartitionDiscovery(context);
                if (context.getSplitsAssignmentSequence().size() < 2) {
                    sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                } else {
                    break;
                }
            }
            verifyLastReadersAssignments(
                    subscriptionType,
                    context,
                    Collections.singleton(dynamicTopic),
                    expectedSplitsAssignmentSequenceSize + 1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void addSplitsBack(SubscriptionType subscriptionType) throws Throwable {
        Set<String> prexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                prexistingTopics,
                                context,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY)) {

            testRegisterReadersForPreexistingTopics(
                    subscriptionType, prexistingTopics, context, enumerator);

            // Simulate a reader failure.
            context.unregisterReader(READER0);
            enumerator.addSplitsBack(
                    context.getSplitsAssignmentSequence().get(0).assignment().get(READER0),
                    READER0);
            int expectedSplitsAssignmentSequenceSize =
                    subscriptionType == SubscriptionType.Failover ? 1 : 2;
            assertThat(context.getSplitsAssignmentSequence())
                    .as("The added back splits should have not been assigned")
                    .hasSize(expectedSplitsAssignmentSequenceSize);

            // Simulate a reader recovery.
            registerReader(context, enumerator, READER0);
            verifyLastReadersAssignments(
                    subscriptionType,
                    context,
                    prexistingTopics,
                    expectedSplitsAssignmentSequenceSize + 1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover"})
    void workWithPreexistingAssignments(SubscriptionType subscriptionType) throws Throwable {
        Set<String> prexistingTopics = setupPreexistingTopics();
        PulsarSourceEnumState preexistingAssignments;
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context1 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                prexistingTopics,
                                context1,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY)) {
            testRegisterReadersForPreexistingTopics(
                    subscriptionType, prexistingTopics, context1, enumerator);
            preexistingAssignments =
                    asEnumState(context1.getSplitsAssignmentSequence().get(0).assignment());
        }

        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context2 =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(
                                subscriptionType,
                                prexistingTopics,
                                context2,
                                ENABLE_PERIODIC_PARTITION_DISCOVERY,
                                preexistingAssignments)) {
            enumerator.start();
            runPeriodicPartitionDiscovery(context2);

            registerReader(context2, enumerator, READER0);
            verifyLastReadersAssignments(subscriptionType, context2, prexistingTopics, 1);
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = SubscriptionType.class,
            names = {"Failover", "Shared"})
    void snapshotState(SubscriptionType subscriptionType) throws Throwable {
        Set<String> prexistingTopics = setupPreexistingTopics();
        try (MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                        new MockSplitEnumeratorContext<>(NUM_SUBTASKS);
                PulsarSourceEnumerator enumerator =
                        createEnumerator(subscriptionType, prexistingTopics, context, false)) {
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
                    getExpectedTopicPartitions(prexistingTopics), state2.getAppendedPartitions());
        }
    }

    private Set<String> setupPreexistingTopics() {
        String topic1 = randomAlphabetic(10);
        String topic2 = randomAlphabetic(10);
        operator().setupTopic(topic1);
        operator().setupTopic(topic2);
        Set<String> preexistingTopics = new HashSet<>();
        preexistingTopics.add(topic1);
        preexistingTopics.add(topic2);
        return preexistingTopics;
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
        verifyLastReadersAssignments(subscriptionType, context, topics, 1);

        registerReader(context, enumerator, READER1);

        int expectedSplitsAssignmentSequenceSize =
                subscriptionType == SubscriptionType.Failover ? 1 : 2;
        verifyLastReadersAssignments(
                subscriptionType, context, topics, expectedSplitsAssignmentSequenceSize);
    }

    private PulsarSourceEnumerator createEnumerator(
            SubscriptionType subscriptionType,
            Set<String> topics,
            MockSplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            boolean enablePeriodicPartitionDiscovery) {
        PulsarSourceEnumState sourceEnumState =
                new PulsarSourceEnumState(
                        Sets.newHashSet(),
                        Sets.newHashSet(),
                        Maps.newHashMap(),
                        Maps.newHashMap(),
                        false);
        return createEnumerator(
                subscriptionType,
                topics,
                enumContext,
                enablePeriodicPartitionDiscovery,
                sourceEnumState);
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
        SourceConfiguration sourceConfiguration = new SourceConfiguration(configuration);

        SplitAssigner assigner =
                SplitAssignerFactory.create(latest(), sourceConfiguration, sourceEnumState);
        return new PulsarSourceEnumerator(
                subscriber,
                StartCursor.earliest(),
                new FullRangeGenerator(),
                configuration,
                sourceConfiguration,
                enumContext,
                assigner);
    }

    private void registerReader(
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumerator enumerator,
            int reader) {
        context.registerReader(new ReaderInfo(reader, "testing location "));
        enumerator.addReader(reader);
    }

    private void verifyLastReadersAssignments(
            SubscriptionType subscriptionType,
            MockSplitEnumeratorContext<PulsarPartitionSplit> context,
            Set<String> topics,
            int expectedAssignmentSeqSize) {
        assertThat(context.getSplitsAssignmentSequence()).hasSize(expectedAssignmentSeqSize);
        verifyAssignments(
                subscriptionType,
                getExpectedTopicPartitions(topics),
                context.getSplitsAssignmentSequence()
                        .get(expectedAssignmentSeqSize - 1)
                        .assignment());
    }

    private void verifyAssignments(
            SubscriptionType subscriptionType,
            Set<TopicPartition> expectedTopicPartitions,
            Map<Integer, List<PulsarPartitionSplit>> actualAssignments) {
        if (subscriptionType == SubscriptionType.Failover) {
            int actualSize = actualAssignments.values().stream().mapToInt(List::size).sum();
            assertThat(actualSize).isEqualTo(expectedTopicPartitions.size());
        } else if (subscriptionType == SubscriptionType.Shared) {
            actualAssignments
                    .values()
                    .forEach(
                            (splits) -> assertThat(splits).hasSize(expectedTopicPartitions.size()));
        }
    }

    private Set<TopicPartition> getExpectedTopicPartitions(Set<String> topics) {
        Set<TopicPartition> allPartitions = new HashSet<>();
        for (String topicName : topics) {
            for (int i = 0; i < PulsarRuntimeOperator.DEFAULT_PARTITIONS; i++) {
                allPartitions.add(new TopicPartition(topicName, i, TopicRange.createFullRange()));
            }
        }
        return allPartitions;
    }

    private void verifySplitAssignmentWithPartitions(
            Set<TopicPartition> expectedAssignment, Set<TopicPartition> actualTopicPartitions) {
        assertThat(actualTopicPartitions).isEqualTo(expectedAssignment);
    }

    // this method only works for non Shared Mode
    private PulsarSourceEnumState asEnumState(
            Map<Integer, List<PulsarPartitionSplit>> assignments) {
        Set<TopicPartition> appendedPartitions = new HashSet<>();
        Set<PulsarPartitionSplit> pendingPartitionSplits = new HashSet<>();
        Map<Integer, Set<PulsarPartitionSplit>> sharedPendingPartitionSplits = new HashMap<>();
        Map<Integer, Set<String>> readerAssignedSplits = new HashMap<>();
        boolean initialized = false;

        assignments
                .values()
                .forEach(
                        splits -> {
                            appendedPartitions.addAll(
                                    splits.stream()
                                            .map(PulsarPartitionSplit::getPartition)
                                            .collect(Collectors.toList()));
                            pendingPartitionSplits.addAll(splits);
                        });

        return new PulsarSourceEnumState(
                appendedPartitions,
                pendingPartitionSplits,
                sharedPendingPartitionSplits,
                readerAssignedSplits,
                initialized);
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
