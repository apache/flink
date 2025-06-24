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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link ShuffleDescriptor}s cache for a {@link ConsumedPartitionGroup}. */
public class CachedShuffleDescriptors {
    /**
     * Stores all serialized shuffle descriptors with indexes. For a partition, it may be added a
     * serialized unknown shuffle descriptor to this list first, and then added the real descriptor
     * later.
     */
    private final List<MaybeOffloaded<ShuffleDescriptorGroup>> serializedShuffleDescriptorGroups;

    /**
     * Stores all to be serialized shuffle descriptors, They will be serialized and added to
     * serializedShuffleDescriptorGroups during the next time TaskDeploymentDescriptor is generated.
     */
    private final Queue<ShuffleDescriptorAndIndex> toBeSerialized;

    /** Stores the mapping of resultPartitionId to index subscripts in consumed partition group. */
    private final Map<IntermediateResultPartitionID, Integer> resultPartitionIdToIndex;

    /** The number of consumers for {@link ConsumedPartitionGroup}. */
    private final int numConsumers;

    public CachedShuffleDescriptors(
            ConsumedPartitionGroup consumedPartitionGroup,
            ShuffleDescriptorAndIndex[] shuffleDescriptors) {
        this.resultPartitionIdToIndex = new HashMap<>();
        this.numConsumers = consumedPartitionGroup.getNumConsumers();
        int index = 0;
        for (IntermediateResultPartitionID resultPartitionID : consumedPartitionGroup) {
            resultPartitionIdToIndex.put(resultPartitionID, index++);
        }
        this.toBeSerialized = new ArrayDeque<>(consumedPartitionGroup.size());
        this.serializedShuffleDescriptorGroups = new ArrayList<>();
        for (ShuffleDescriptorAndIndex shuffleDescriptor : shuffleDescriptors) {
            toBeSerialized.offer(shuffleDescriptor);
        }
    }

    public List<MaybeOffloaded<ShuffleDescriptorGroup>> getAllSerializedShuffleDescriptorGroups() {
        // the deployment of task is not executed in jobMaster's main thread, copy this list to
        // avoid new element added to the serializedShuffleDescriptors before TDD is not serialized.
        return new ArrayList<>(serializedShuffleDescriptorGroups);
    }

    public void serializeShuffleDescriptors(
            TaskDeploymentDescriptorFactory.ShuffleDescriptorSerializer shuffleDescriptorSerializer)
            throws IOException {
        if (!toBeSerialized.isEmpty()) {
            ShuffleDescriptorGroup shuffleDescriptorGroup =
                    new ShuffleDescriptorGroup(
                            toBeSerialized.toArray(new ShuffleDescriptorAndIndex[0]));
            MaybeOffloaded<ShuffleDescriptorGroup> serializedShuffleDescriptorGroup =
                    shuffleDescriptorSerializer.serializeAndTryOffloadShuffleDescriptor(
                            shuffleDescriptorGroup, numConsumers);

            toBeSerialized.clear();
            serializedShuffleDescriptorGroups.add(serializedShuffleDescriptorGroup);
        }
    }

    public void markPartitionFinished(IntermediateResultPartition resultPartition) {
        ShuffleDescriptor consumedPartitionShuffleDescriptor =
                TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor(
                        resultPartition,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.MUST_BE_KNOWN,
                        // because resultPartition is already finished, false is fair enough.
                        false);
        toBeSerialized.offer(
                new ShuffleDescriptorAndIndex(
                        consumedPartitionShuffleDescriptor,
                        checkNotNull(
                                resultPartitionIdToIndex.get(resultPartition.getPartitionId()))));
    }
}
