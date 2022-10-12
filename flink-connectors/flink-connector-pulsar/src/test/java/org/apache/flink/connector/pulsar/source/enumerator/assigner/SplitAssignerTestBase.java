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

import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState.initialState;
import static org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor.defaultStopCursor;
import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for split assigners. */
abstract class SplitAssignerTestBase extends TestLogger {

    private static final List<MockSplitEnumeratorContext<PulsarPartitionSplit>> enumeratorContexts =
            new ArrayList<>();

    @Test
    void registerTopicPartitionsWillOnlyReturnNewPartitions() {
        SplitAssigner assigner = splitAssigner(true, 4);

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
        SplitAssigner assigner = splitAssigner(false, 4);
        assigner.registerTopicPartitions(createPartitions("c", 5));

        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment =
                assigner.createAssignment(emptyList());
        assertThat(assignment).isNotPresent();
    }

    @Test
    void noPartitionsProvideForAssignment() {
        SplitAssigner assigner = splitAssigner(true, 4);
        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment =
                assigner.createAssignment(singletonList(4));
        assertThat(assignment).isNotPresent();
    }

    protected Set<TopicPartition> createPartitions(String topic, int partitionId) {
        TopicPartition p1 = new TopicPartition(topic, partitionId);
        return singleton(p1);
    }

    protected SplitAssigner splitAssigner(boolean discovery, int parallelism) {
        MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                new MockSplitEnumeratorContext<>(parallelism);
        enumeratorContexts.add(context);
        return createAssigner(defaultStopCursor(), discovery, context, initialState());
    }

    protected SplitAssigner splitAssigner(
            boolean discovery, int parallelism, Set<TopicPartition> partitions) {
        MockSplitEnumeratorContext<PulsarPartitionSplit> context =
                new MockSplitEnumeratorContext<>(parallelism);
        enumeratorContexts.add(context);
        return createAssigner(
                defaultStopCursor(), discovery, context, new PulsarSourceEnumState(partitions));
    }

    protected abstract SplitAssigner createAssigner(
            StopCursor stopCursor,
            boolean enablePartitionDiscovery,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumState enumState);

    @AfterAll
    static void afterAll() throws Exception {
        for (MockSplitEnumeratorContext<PulsarPartitionSplit> context : enumeratorContexts) {
            context.close();
        }
    }
}
