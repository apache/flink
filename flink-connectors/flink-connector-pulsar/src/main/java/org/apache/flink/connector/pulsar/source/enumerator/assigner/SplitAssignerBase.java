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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Common abstraction for split assigner. */
abstract class SplitAssignerBase implements SplitAssigner {

    protected final StopCursor stopCursor;
    protected final boolean enablePartitionDiscovery;
    protected final SplitEnumeratorContext<PulsarPartitionSplit> context;
    protected final Set<TopicPartition> appendedPartitions;
    protected final Map<Integer, Set<PulsarPartitionSplit>> pendingPartitionSplits;

    protected boolean initialized;

    protected SplitAssignerBase(
            StopCursor stopCursor,
            boolean enablePartitionDiscovery,
            SplitEnumeratorContext<PulsarPartitionSplit> context,
            PulsarSourceEnumState enumState) {
        this.stopCursor = stopCursor;
        this.enablePartitionDiscovery = enablePartitionDiscovery;
        this.context = context;
        this.appendedPartitions = enumState.getAppendedPartitions();
        this.pendingPartitionSplits = new HashMap<>(context.currentParallelism());
        this.initialized = false;
    }

    @Override
    public Optional<SplitsAssignment<PulsarPartitionSplit>> createAssignment(
            List<Integer> readers) {
        if (pendingPartitionSplits.isEmpty() || readers.isEmpty()) {
            return Optional.empty();
        }

        Map<Integer, List<PulsarPartitionSplit>> assignMap =
                new HashMap<>(pendingPartitionSplits.size());

        for (Integer reader : readers) {
            Set<PulsarPartitionSplit> splits = pendingPartitionSplits.remove(reader);
            if (splits != null && !splits.isEmpty()) {
                assignMap.put(reader, new ArrayList<>(splits));
            }
        }

        if (assignMap.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new SplitsAssignment<>(assignMap));
        }
    }

    @Override
    public boolean noMoreSplits(Integer reader) {
        return !enablePartitionDiscovery
                && initialized
                && !pendingPartitionSplits.containsKey(reader);
    }

    @Override
    public PulsarSourceEnumState snapshotState() {
        return new PulsarSourceEnumState(appendedPartitions);
    }

    @Override
    public long getUnassignedSplitCount() {
        return pendingPartitionSplits.values().stream().mapToLong(Set::size).sum();
    }

    /** Add split to pending lists. */
    protected void addSplitToPendingList(int readerId, PulsarPartitionSplit split) {
        Set<PulsarPartitionSplit> splits =
                pendingPartitionSplits.computeIfAbsent(readerId, i -> new HashSet<>());
        splits.add(split);
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
    protected int partitionOwner(TopicPartition partition) {
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
