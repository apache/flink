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

package org.apache.flink.connector.pulsar.sink.writer.topic.register;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicExtractor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Unit tests for {@link DynamicTopicRegister}. */
class DynamicTopicRegisterTest extends PulsarTestSuiteBase {

    private final MockTopicExtractor extractor = new MockTopicExtractor();

    private static final class MockTopicExtractor implements TopicExtractor<String> {
        private static final long serialVersionUID = 2456172645787498006L;

        private TopicPartition partition;

        @Override
        public TopicPartition extract(String s, TopicMetadataProvider provider) {
            return partition;
        }

        public void setPartition(TopicPartition partition) {
            this.partition = partition;
        }
    }

    @Test
    void partitionedTopicWouldBeReturnedDirectly() throws IOException {
        DynamicTopicRegister<String> register = topicRegister(50000);
        TopicPartition partition = new TopicPartition("some", 1);
        extractor.setPartition(partition);
        List<String> topics = register.topics(randomAlphabetic(10));

        assertThat(topics)
                .hasSize(1)
                .allSatisfy(topic -> assertThat(topic).isEqualTo(partition.getFullTopicName()));

        register.close();
    }

    @Test
    void rootTopicWillReturnAllThePartitions() throws IOException {
        DynamicTopicRegister<String> register = topicRegister(50000);
        TopicPartition partition = new TopicPartition("root-topic" + randomAlphabetic(10));
        extractor.setPartition(partition);
        operator().createTopic(partition.getFullTopicName(), 10);
        List<String> topics = register.topics(randomAlphabetic(10));

        assertThat(topics)
                .hasSize(10)
                .allSatisfy(topic -> assertThat(topic).startsWith(partition.getTopic()));

        register.close();
    }

    private DynamicTopicRegister<String> topicRegister(long interval) {
        DynamicTopicRegister<String> register = new DynamicTopicRegister<>(extractor);
        register.open(sinkConfiguration(interval), mock(ProcessingTimeService.class));

        return register;
    }

    private SinkConfiguration sinkConfiguration(long interval) {
        Configuration configuration = operator().config();
        configuration.set(PULSAR_TOPIC_METADATA_REFRESH_INTERVAL, interval);

        return new SinkConfiguration(configuration);
    }
}
