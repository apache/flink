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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.apache.flink.connector.pulsar.source.enumerator.assigner.SplitAssignerBase.calculatePartitionOwner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link NonSharedSplitAssigner}. */
class NonSharedSplitAssignerTest extends SplitAssignerTestBase {

    @Test
    void noMoreSplits() {
        SplitAssigner assigner = splitAssigner(true, 4);
        assertFalse(assigner.noMoreSplits(3));

        assigner = splitAssigner(false, 4);
        assertFalse(assigner.noMoreSplits(3));

        Set<TopicPartition> partitions = createPartitions("persistent://public/default/f", 8);
        int owner = calculatePartitionOwner("persistent://public/default/f", 8, 4);

        assigner.registerTopicPartitions(partitions);
        assertFalse(assigner.noMoreSplits(owner));

        assigner.createAssignment(singletonList(owner));
        assertTrue(assigner.noMoreSplits(owner));
    }

    @Test
    void partitionsAssignment() {
        SplitAssigner assigner = splitAssigner(true, 4);
        assigner.registerTopicPartitions(createPartitions("persistent://public/default/d", 4));
        int owner = calculatePartitionOwner("persistent://public/default/d", 4, 4);
        List<Integer> readers = Arrays.asList(owner, owner + 1);

        // Assignment with initial states.
        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment =
                assigner.createAssignment(readers);
        assertThat(assignment).isPresent();
        assertThat(assignment.get().assignment()).hasSize(1);

        // Reassignment with the same readers.
        assignment = assigner.createAssignment(readers);
        assertThat(assignment).isNotPresent();

        // Register new partition and assign.
        assigner.registerTopicPartitions(createPartitions("persistent://public/default/e", 5));
        assigner.registerTopicPartitions(createPartitions("persistent://public/default/f", 1));
        assigner.registerTopicPartitions(createPartitions("persistent://public/default/g", 3));
        assigner.registerTopicPartitions(createPartitions("persistent://public/default/h", 4));

        Set<Integer> owners = new HashSet<>();
        owners.add(calculatePartitionOwner("persistent://public/default/e", 5, 4));
        owners.add(calculatePartitionOwner("persistent://public/default/f", 1, 4));
        owners.add(calculatePartitionOwner("persistent://public/default/g", 3, 4));
        owners.add(calculatePartitionOwner("persistent://public/default/h", 4, 4));
        readers = new ArrayList<>(owners);

        assignment = assigner.createAssignment(readers);
        assertThat(assignment).isPresent();
        assertThat(assignment.get().assignment()).hasSize(readers.size());

        // Assign to new readers.
        readers = Collections.singletonList(5);
        assignment = assigner.createAssignment(readers);
        assertThat(assignment).isNotPresent();
    }

    @Override
    protected SplitAssigner createAssigner(
            StopCursor stopCursor,
            boolean enablePartitionDiscovery,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumState enumState) {
        return new NonSharedSplitAssigner(stopCursor, enablePartitionDiscovery, context, enumState);
    }
}
