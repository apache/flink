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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * This assigner is used for {@link SubscriptionType#Failover}, {@link SubscriptionType#Exclusive}
 * and {@link SubscriptionType#Key_Shared} subscriptions.
 */
@Internal
class NonSharedSplitAssigner extends SplitAssignerBase {

    public NonSharedSplitAssigner(
            StopCursor stopCursor,
            boolean enablePartitionDiscovery,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumState enumState) {
        super(stopCursor, enablePartitionDiscovery, context, enumState);
    }

    @Override
    public List<TopicPartition> registerTopicPartitions(Set<TopicPartition> fetchedPartitions) {
        List<TopicPartition> newPartitions = new ArrayList<>();

        for (TopicPartition partition : fetchedPartitions) {
            if (!appendedPartitions.contains(partition)) {
                appendedPartitions.add(partition);
                newPartitions.add(partition);

                // Calculate the reader id by the current parallelism.
                int readerId = partitionOwner(partition);
                PulsarPartitionSplit split = new PulsarPartitionSplit(partition, stopCursor);
                addSplitToPendingList(readerId, split);
            }
        }

        if (!initialized) {
            initialized = true;
        }

        return newPartitions;
    }

    @Override
    public void addSplitsBack(List<PulsarPartitionSplit> splits, int subtaskId) {
        for (PulsarPartitionSplit split : splits) {
            int readerId = partitionOwner(split.getPartition());
            addSplitToPendingList(readerId, split);
        }
    }

    /**
     * Returns the index of the target subtask that a specific partition should be assigned to. It's
     * inspired by the {@code KafkaSourceEnumerator.getSplitOwner()}
     *
     * <p>The resulting distribution of partition has the following contract:
     *
     * <ul>
     *   <li>1. Uniformly distributed across subtasks.
     *   <li>2. Partitions are round-robin distributed (strictly clockwise w.r.t. ascending subtask
     *       indices) by using the partition id as the offset from a starting index (i.e., the index
     *       of the subtask which partition 0 of the topic will be assigned to, determined using the
     *       topic name).
     * </ul>
     *
     * @param partition The Pulsar partition to assign.
     * @return The id of the reader that owns this partition.
     */
    private int partitionOwner(TopicPartition partition) {
        return calculatePartitionOwner(
                partition.getTopic(), partition.getPartitionId(), context.currentParallelism());
    }

    @VisibleForTesting
    static int calculatePartitionOwner(String topic, int partitionId, int parallelism) {
        int startIndex = ((topic.hashCode() * 31) & 0x7FFFFFFF) % parallelism;
        /*
         * Here, the assumption is that the id of Pulsar partitions are always ascending starting from
         * 0. Therefore, can be used directly as the offset clockwise from the start index.
         */
        return (startIndex + partitionId) % parallelism;
    }
}
