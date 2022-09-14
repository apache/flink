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

package org.apache.flink.connector.pulsar.source.enumerator.subscriber;

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicListSubscriber;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicPatternSubscriber;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicName;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.AllTopics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link PulsarSubscriber}. */
class PulsarSubscriberTest extends PulsarTestSuiteBase {

    private final String topic1 = topicName("topic-" + randomAlphanumeric(4));
    private final String topic2 = topicName("pattern-topic-" + randomAlphanumeric(4));
    private final String topic3 = topicName("topic2-" + randomAlphanumeric(4));
    private final String topic4 = topicName("non-partitioned-topic-" + randomAlphanumeric(4));
    private final String topic5 = topicName("non-partitioned-topic2-" + randomAlphanumeric(4));

    private static final int NUM_PARTITIONS_PER_TOPIC = 5;
    private static final int NUM_PARALLELISM = 10;

    @BeforeAll
    void setUp() {
        operator().createTopic(topic1, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic2, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic3, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(topic4, 0);
        operator().createTopic(topic5, 0);
    }

    @AfterAll
    void tearDown() {
        operator().deleteTopic(topic1);
        operator().deleteTopic(topic2);
        operator().deleteTopic(topic3);
        operator().deleteTopic(topic4);
        operator().deleteTopic(topic5);
    }

    @Test
    void topicListSubscriber() {
        PulsarSubscriber subscriber = getTopicListSubscriber(Arrays.asList(topic1, topic2));
        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(
                        operator().admin(), new FullRangeGenerator(), NUM_PARALLELISM);
        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(topic1, i, createFullRange()));
            expectedPartitions.add(new TopicPartition(topic2, i, createFullRange()));
        }

        assertEquals(expectedPartitions, topicPartitions);
    }

    @Test
    void subscribeOnePartitionOfMultiplePartitionTopic() {
        String partition = topicNameWithPartition(topic1, 2);

        PulsarSubscriber subscriber = getTopicListSubscriber(singletonList(partition));
        Set<TopicPartition> partitions =
                subscriber.getSubscribedTopicPartitions(
                        operator().admin(), new FullRangeGenerator(), NUM_PARALLELISM);

        TopicPartition desiredPartition = new TopicPartition(topic1, 2, createFullRange());
        assertThat(partitions).hasSize(1).containsExactly(desiredPartition);
    }

    @Test
    void subscribeNonPartitionedTopicList() {
        PulsarSubscriber subscriber = getTopicListSubscriber(singletonList(topic4));
        Set<TopicPartition> partitions =
                subscriber.getSubscribedTopicPartitions(
                        operator().admin(), new FullRangeGenerator(), NUM_PARALLELISM);

        TopicPartition desiredPartition = new TopicPartition(topic4, -1, createFullRange());
        assertThat(partitions).hasSize(1).containsExactly(desiredPartition);
    }

    @Test
    void subscribeNonPartitionedTopicPattern() {
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(
                        Pattern.compile("persistent://public/default/non-partitioned-topic*?"),
                        AllTopics);

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(
                        operator().admin(), new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();

        expectedPartitions.add(new TopicPartition(topic4, -1, createFullRange()));
        expectedPartitions.add(new TopicPartition(topic5, -1, createFullRange()));

        assertEquals(expectedPartitions, topicPartitions);
    }

    @Test
    void topicPatternSubscriber() {
        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(
                        Pattern.compile("persistent://public/default/topic*?"), AllTopics);

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(
                        operator().admin(), new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(topic1, i, createFullRange()));
            expectedPartitions.add(new TopicPartition(topic3, i, createFullRange()));
        }

        assertEquals(expectedPartitions, topicPartitions);
    }
}
