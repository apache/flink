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

package org.apache.flink.connector.pulsar.sink.writer.topic;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithoutPartition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link TopicMetadataListener}. */
class TopicMetadataListenerTest extends PulsarTestSuiteBase {

    @Test
    void listenEmptyTopics() {
        TopicMetadataListener listener = new TopicMetadataListener();
        SinkConfiguration configuration = sinkConfiguration(Duration.ofMinutes(5).toMillis());
        TestProcessingTimeService timeService = new TestProcessingTimeService();

        List<String> topics = listener.availableTopics();
        assertThat(topics).isEmpty();

        listener.open(configuration, timeService);
        topics = listener.availableTopics();
        assertThat(topics).isEmpty();
    }

    @Test
    void listenOnPartitions() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 6);
        List<String> partitions = topicPartitions(topic, 6);

        TopicMetadataListener listener = new TopicMetadataListener(partitions);
        long interval = Duration.ofMinutes(15).toMillis();
        SinkConfiguration configuration = sinkConfiguration(interval);
        TestProcessingTimeService timeService = new TestProcessingTimeService();

        List<String> topics = listener.availableTopics();
        assertEquals(topics, partitions);

        listener.open(configuration, timeService);
        topics = listener.availableTopics();
        assertEquals(topics, partitions);

        operator().increaseTopicPartitions(topic, 12);
        timeService.advance(interval);
        topics = listener.availableTopics();
        assertEquals(topics, partitions);
    }

    @Test
    void fetchTopicPartitionInformation() {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        TopicMetadataListener listener = new TopicMetadataListener(singletonList(topic));
        SinkConfiguration configuration = sinkConfiguration(Duration.ofMinutes(10).toMillis());
        TestProcessingTimeService timeService = new TestProcessingTimeService();

        List<String> topics = listener.availableTopics();
        assertThat(topics).isEmpty();

        listener.open(configuration, timeService);
        topics = listener.availableTopics();
        List<String> desiredTopics = topicPartitions(topic, 8);

        assertThat(topics).hasSize(8).isEqualTo(desiredTopics);
    }

    @Test
    void fetchTopicPartitionUpdate() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        long interval = Duration.ofMinutes(20).toMillis();

        TopicMetadataListener listener = new TopicMetadataListener(singletonList(topic));
        SinkConfiguration configuration = sinkConfiguration(interval);
        TestProcessingTimeService timeService = new TestProcessingTimeService();
        timeService.setCurrentTime(System.currentTimeMillis());

        listener.open(configuration, timeService);
        List<String> topics = listener.availableTopics();
        List<String> desiredTopics = topicPartitions(topic, 8);

        assertThat(topics).isEqualTo(desiredTopics);

        // Increase topic partitions and trigger the metadata update logic.
        operator().increaseTopicPartitions(topic, 16);
        timeService.advance(interval);

        topics = listener.availableTopics();
        desiredTopics = topicPartitions(topic, 16);
        assertThat(topics).isEqualTo(desiredTopics);
    }

    @Test
    void fetchNonPartitionTopic() {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 0);
        List<String> nonPartitionTopic =
                Collections.singletonList(topicNameWithoutPartition(topic));

        TopicMetadataListener listener = new TopicMetadataListener(nonPartitionTopic);
        long interval = Duration.ofMinutes(15).toMillis();
        SinkConfiguration configuration = sinkConfiguration(interval);
        TestProcessingTimeService timeService = new TestProcessingTimeService();

        listener.open(configuration, timeService);
        List<String> topics = listener.availableTopics();
        assertEquals(topics, nonPartitionTopic);
    }

    private List<String> topicPartitions(String topic, int partitionSize) {
        return IntStream.range(0, partitionSize)
                .boxed()
                .map(i -> topicNameWithPartition(topic, i))
                .collect(toList());
    }

    private SinkConfiguration sinkConfiguration(long interval) {
        Configuration configuration = operator().config();
        configuration.set(PULSAR_TOPIC_METADATA_REFRESH_INTERVAL, interval);

        return new SinkConfiguration(configuration);
    }
}
