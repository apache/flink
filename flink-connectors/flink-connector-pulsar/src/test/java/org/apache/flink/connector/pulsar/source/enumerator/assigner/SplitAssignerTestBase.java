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

package org.apache.flink.connector.pulsar.source.enumerator.assigner;

import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState.initialState;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor.defaultStopCursor;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for split assigners. */
abstract class SplitAssignerTestBase<T extends SplitAssigner> extends TestLogger {

    @Test
    void registerTopicPartitionsWillOnlyReturnNewPartitions() {
        T assigner = splitAssigner(true);

        Set<TopicPartition> partitions = createPartitions("persistent://public/default/a", 1);
        List<TopicPartition> newPartitions = assigner.registerTopicPartitions(partitions);
        assertThat(newPartitions)
                .hasSize(1)
                .first()
                .hasFieldOrPropertyWithValue("topic", "persistent://public/default/a")
                .hasFieldOrPropertyWithValue("partitionId", 1);

        newPartitions = assigner.registerTopicPartitions(partitions);
        assertThat(newPartitions).isEmpty();

        partitions = createPartitions("persistent://public/default/b", 2);
        newPartitions = assigner.registerTopicPartitions(partitions);
        assertThat(newPartitions)
                .hasSize(1)
                .hasSize(1)
                .first()
                .hasFieldOrPropertyWithValue("topic", "persistent://public/default/b")
                .hasFieldOrPropertyWithValue("partitionId", 2);
    }

    @Test
    void noReadersProvideForAssignment() {
        T assigner = splitAssigner(false);
        assigner.registerTopicPartitions(createPartitions("c", 5));

        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment =
                assigner.createAssignment(emptyList());
        assertThat(assignment).isNotPresent();
    }

    @Test
    void noPartitionsProvideForAssignment() {
        T assigner = splitAssigner(true);
        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment =
                assigner.createAssignment(singletonList(4));
        assertThat(assignment).isNotPresent();
    }

    protected Set<TopicPartition> createPartitions(String topic, int partitionId) {
        TopicPartition p1 = new TopicPartition(topic, partitionId, createFullRange());
        return singleton(p1);
    }

    protected T splitAssigner(boolean discovery) {
        Configuration configuration = new Configuration();

        if (discovery) {
            configuration.set(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 1000L);
        } else {
            configuration.set(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, -1L);
        }

        SourceConfiguration sourceConfiguration = new SourceConfiguration(configuration);
        return createAssigner(defaultStopCursor(), sourceConfiguration, initialState());
    }

    protected abstract T createAssigner(
            StopCursor stopCursor,
            SourceConfiguration sourceConfiguration,
            PulsarSourceEnumState sourceEnumState);
}
