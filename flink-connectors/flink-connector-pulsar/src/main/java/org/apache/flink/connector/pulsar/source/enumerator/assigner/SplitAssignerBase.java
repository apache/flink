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

    /** Add split to pending lists. */
    protected void addSplitToPendingList(int readerId, PulsarPartitionSplit split) {
        Set<PulsarPartitionSplit> splits =
                pendingPartitionSplits.computeIfAbsent(readerId, i -> new HashSet<>());
        splits.add(split);
    }
}
