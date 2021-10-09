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

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.FullRangeGenerator;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicListSubscriber;
import static org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber.getTopicPatternSubscriber;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.apache.pulsar.client.api.RegexSubscriptionMode.AllTopics;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link PulsarSubscriber}. */
class PulsarSubscriberTest extends PulsarTestSuiteBase {

    private static final String TOPIC1 = TopicNameUtils.topicName("topic1");
    private static final String TOPIC2 = TopicNameUtils.topicName("pattern-topic");
    private static final String TOPIC3 = TopicNameUtils.topicName("topic2");

    private static final int NUM_PARTITIONS_PER_TOPIC = 5;
    private static final int NUM_PARALLELISM = 10;

    @Test
    void topicListSubscriber() {
        operator().createTopic(TOPIC1, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(TOPIC2, NUM_PARTITIONS_PER_TOPIC);

        PulsarSubscriber subscriber = getTopicListSubscriber(Arrays.asList(TOPIC1, TOPIC2));
        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(
                        operator().admin(), new FullRangeGenerator(), NUM_PARALLELISM);
        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(TOPIC1, i, createFullRange()));
            expectedPartitions.add(new TopicPartition(TOPIC2, i, createFullRange()));
        }

        assertEquals(expectedPartitions, topicPartitions);

        operator().deleteTopic(TOPIC1, true);
        operator().deleteTopic(TOPIC2, true);
    }

    @Test
    void topicPatternSubscriber() {
        operator().createTopic(TOPIC1, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(TOPIC2, NUM_PARTITIONS_PER_TOPIC);
        operator().createTopic(TOPIC3, NUM_PARTITIONS_PER_TOPIC);

        PulsarSubscriber subscriber =
                getTopicPatternSubscriber(
                        Pattern.compile("persistent://public/default/topic*?"), AllTopics);

        Set<TopicPartition> topicPartitions =
                subscriber.getSubscribedTopicPartitions(
                        operator().admin(), new FullRangeGenerator(), NUM_PARALLELISM);

        Set<TopicPartition> expectedPartitions = new HashSet<>();

        for (int i = 0; i < NUM_PARTITIONS_PER_TOPIC; i++) {
            expectedPartitions.add(new TopicPartition(TOPIC1, i, createFullRange()));
            expectedPartitions.add(new TopicPartition(TOPIC3, i, createFullRange()));
        }

        assertEquals(expectedPartitions, topicPartitions);

        operator().deleteTopic(TOPIC1, true);
        operator().deleteTopic(TOPIC2, true);
        operator().deleteTopic(TOPIC3, true);
    }
}
