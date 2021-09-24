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

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.apache.flink.shaded.guava30.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor.latest;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicPatternSubscriber;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link PulsarSourceEnumerator}. */
class PulsarSourceEnumeratorTest extends PulsarTestSuiteBase {

    private static final int NUM_SUBTASKS = 3;
    private static final String DYNAMIC_TOPIC_NAME = "dynamic_topic";
    private static final String TOPIC1 = "topic";
    private static final String TOPIC2 = "pattern-topic";
    private static final Set<String> PRE_EXISTING_TOPICS = Sets.newHashSet(TOPIC1, TOPIC2);
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
            assertTrue(context.getPeriodicCallables().isEmpty());
            assertEquals(
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
            assertTrue(context.getOneTimeCallables().isEmpty());
            assertEquals(
                    1,
                    context.getPeriodicCallables().size(),
                    "A periodic partition discovery callable should have been scheduled");
        }
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
        List<String> topics = new ArrayList<>(PRE_EXISTING_TOPICS);
        if (includeDynamicTopic) {
            topics.add(DYNAMIC_TOPIC_NAME);
        }
        Configuration configuration = operator().config();
        configuration.set(PULSAR_SUBSCRIPTION_TYPE, SubscriptionType.Failover);

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
}
