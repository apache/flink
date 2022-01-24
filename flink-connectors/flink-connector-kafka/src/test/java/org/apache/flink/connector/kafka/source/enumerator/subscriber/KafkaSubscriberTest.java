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

package org.apache.flink.connector.kafka.source.enumerator.subscriber;

import org.apache.flink.connector.kafka.testutils.annotations.Kafka;
import org.apache.flink.connector.kafka.testutils.annotations.KafkaKit;
import org.apache.flink.connector.kafka.testutils.annotations.Topic;
import org.apache.flink.connector.kafka.testutils.extension.KafkaClientKit;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Unit tests for {@link KafkaSubscriber}. */
@ExtendWith(TestLoggerExtension.class)
@Kafka
class KafkaSubscriberTest {
    @Topic private static final String TOPIC1 = "topic1";
    @Topic private static final String TOPIC2 = "pattern-topic";
    private static final TopicPartition NON_EXISTING_TOPIC = new TopicPartition("removed", 0);

    @KafkaKit KafkaClientKit kafkaClientKit;

    @Test
    void testTopicListSubscriber() throws Exception {
        List<String> topics = Arrays.asList(TOPIC1, TOPIC2);
        KafkaSubscriber subscriber =
                KafkaSubscriber.getTopicListSubscriber(Arrays.asList(TOPIC1, TOPIC2));
        final Set<TopicPartition> subscribedPartitions =
                subscriber.getSubscribedTopicPartitions(kafkaClientKit.getAdminClient());

        final Set<TopicPartition> expectedSubscribedPartitions =
                new HashSet<>(kafkaClientKit.getPartitionsForTopics(topics));

        assertThat(subscribedPartitions).isEqualTo(expectedSubscribedPartitions);
    }

    @Test
    void testNonExistingTopic() {
        final KafkaSubscriber subscriber =
                KafkaSubscriber.getTopicListSubscriber(
                        Collections.singletonList(NON_EXISTING_TOPIC.topic()));

        Throwable t =
                catchThrowable(
                        () ->
                                subscriber.getSubscribedTopicPartitions(
                                        kafkaClientKit.getAdminClient()));
        assertThat(t).isInstanceOf(RuntimeException.class);
        assertThat(ExceptionUtils.findThrowable(t, UnknownTopicOrPartitionException.class))
                .as("Exception should be caused by UnknownTopicOrPartitionException")
                .isPresent();
    }

    @Test
    void testTopicPatternSubscriber() throws Exception {
        KafkaSubscriber subscriber =
                KafkaSubscriber.getTopicPatternSubscriber(Pattern.compile("pattern.*"));
        final Set<TopicPartition> subscribedPartitions =
                subscriber.getSubscribedTopicPartitions(kafkaClientKit.getAdminClient());

        final Set<TopicPartition> expectedSubscribedPartitions =
                new HashSet<>(kafkaClientKit.getPartitionsForTopics(Collections.singleton(TOPIC2)));

        assertThat(subscribedPartitions).isEqualTo(expectedSubscribedPartitions);
    }

    @Test
    void testPartitionSetSubscriber() throws Exception {
        List<String> topics = Arrays.asList(TOPIC1, TOPIC2);
        Set<TopicPartition> partitions =
                new HashSet<>(kafkaClientKit.getPartitionsForTopics(topics));
        partitions.remove(new TopicPartition(TOPIC1, 1));

        KafkaSubscriber subscriber = KafkaSubscriber.getPartitionSetSubscriber(partitions);

        final Set<TopicPartition> subscribedPartitions =
                subscriber.getSubscribedTopicPartitions(kafkaClientKit.getAdminClient());

        assertThat(subscribedPartitions).isEqualTo(partitions);
    }

    @Test
    void testNonExistingPartition() {
        TopicPartition nonExistingPartition = new TopicPartition(TOPIC1, Integer.MAX_VALUE);
        final KafkaSubscriber subscriber =
                KafkaSubscriber.getPartitionSetSubscriber(
                        Collections.singleton(nonExistingPartition));

        assertThatThrownBy(
                        () ->
                                subscriber.getSubscribedTopicPartitions(
                                        kafkaClientKit.getAdminClient()))
                .isInstanceOf(RuntimeException.class)
                .hasMessage(
                        String.format(
                                "Partition '%s' does not exist on Kafka brokers",
                                nonExistingPartition));
    }
}
