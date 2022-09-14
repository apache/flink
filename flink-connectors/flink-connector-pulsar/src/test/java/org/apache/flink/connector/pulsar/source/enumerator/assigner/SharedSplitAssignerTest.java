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
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link SharedSplitAssigner}. */
class SharedSplitAssignerTest extends SplitAssignerTestBase {

    @Test
    void noMoreSplits() {
        SplitAssigner assigner = splitAssigner(true, 4);
        assertFalse(assigner.noMoreSplits(3));

        assigner = splitAssigner(false, 4);
        assertFalse(assigner.noMoreSplits(3));

        assigner.registerTopicPartitions(createPartitions("f", 8));
        assertFalse(assigner.noMoreSplits(3));

        assigner.createAssignment(singletonList(1));
        assertTrue(assigner.noMoreSplits(1));
        assertFalse(assigner.noMoreSplits(3));

        assigner.createAssignment(singletonList(3));
        assertTrue(assigner.noMoreSplits(3));
    }

    @Test
    void partitionsAssignment() {
        SplitAssigner assigner = splitAssigner(true, 8);
        assigner.registerTopicPartitions(createPartitions("d", 4));
        List<Integer> readers = Arrays.asList(1, 3, 5, 7);

        // Assignment with initial states.
        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment =
                assigner.createAssignment(readers);
        assertThat(assignment).isPresent();
        assertThat(assignment.get().assignment()).hasSize(4);

        // Reassignment with same readers.
        assignment = assigner.createAssignment(readers);
        assertThat(assignment).isNotPresent();

        // Register new partition and assign.
        assigner.registerTopicPartitions(createPartitions("e", 5));
        assignment = assigner.createAssignment(readers);
        assertThat(assignment).isPresent();
        assertThat(assignment.get().assignment()).hasSize(4);

        // Assign to new readers.
        readers = Arrays.asList(0, 2, 4, 6);
        assignment = assigner.createAssignment(readers);
        assertThat(assignment).isPresent();
        assertThat(assignment.get().assignment())
                .hasSize(4)
                .allSatisfy((k, v) -> assertThat(v).hasSize(2));
    }

    @Test
    void reassignSplitsAfterRestarting() {
        SplitAssigner assigner = splitAssigner(true, 8);
        Set<TopicPartition> partitions = createPartitions("d", 4);
        assigner.registerTopicPartitions(partitions);
        List<Integer> readers = Arrays.asList(0, 1, 2);

        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment =
                assigner.createAssignment(readers);
        assertThat(assignment).isPresent();
        assertThat(assignment.get().assignment()).hasSize(3);

        // Create a new split assigner with same state.
        SplitAssigner assigner1 = splitAssigner(true, 8, partitions);
        assigner1.registerTopicPartitions(partitions);
        assignment = assigner1.createAssignment(readers);
        assertThat(assignment).isPresent();
        assertThat(assignment.get().assignment()).hasSize(3);
    }

    @Override
    protected SplitAssigner createAssigner(
            StopCursor stopCursor,
            boolean enablePartitionDiscovery,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumState enumState) {
        return new SharedSplitAssigner(stopCursor, enablePartitionDiscovery, context, enumState);
    }
}
