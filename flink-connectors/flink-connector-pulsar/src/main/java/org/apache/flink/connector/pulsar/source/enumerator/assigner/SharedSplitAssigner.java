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
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** This assigner is used for {@link SubscriptionType#Shared} subscriptions. */
@Internal
public class SharedSplitAssigner implements SplitAssigner {
    private static final long serialVersionUID = 8468503133499402491L;

    private final StopCursor stopCursor;
    private final boolean enablePartitionDiscovery;

    // These fields would be saved into checkpoint.

    private final Set<TopicPartition> appendedPartitions;
    private final Map<Integer, Set<PulsarPartitionSplit>> sharedPendingPartitionSplits;
    private final Map<Integer, Set<String>> readerAssignedSplits;
    private boolean initialized;

    public SharedSplitAssigner(
            StopCursor stopCursor,
            SourceConfiguration sourceConfiguration,
            PulsarSourceEnumState sourceEnumState) {
        this.stopCursor = stopCursor;
        this.enablePartitionDiscovery = sourceConfiguration.isEnablePartitionDiscovery();
        this.appendedPartitions = sourceEnumState.getAppendedPartitions();
        this.sharedPendingPartitionSplits = sourceEnumState.getSharedPendingPartitionSplits();
        this.readerAssignedSplits = sourceEnumState.getReaderAssignedSplits();
        this.initialized = sourceEnumState.isInitialized();
    }

    @Override
    public List<TopicPartition> registerTopicPartitions(Set<TopicPartition> fetchedPartitions) {
        List<TopicPartition> newPartitions = new ArrayList<>();

        for (TopicPartition partition : fetchedPartitions) {
            if (!appendedPartitions.contains(partition)) {
                appendedPartitions.add(partition);
                newPartitions.add(partition);
            }
        }

        if (!initialized) {
            initialized = true;
        }

        return newPartitions;
    }

    @Override
    public void addSplitsBack(List<PulsarPartitionSplit> splits, int subtaskId) {
        Set<PulsarPartitionSplit> pendingPartitionSplits =
                sharedPendingPartitionSplits.computeIfAbsent(subtaskId, id -> new HashSet<>());
        pendingPartitionSplits.addAll(splits);
    }

    @Override
    public Optional<SplitsAssignment<PulsarPartitionSplit>> createAssignment(
            List<Integer> readers) {
        if (readers.isEmpty()) {
            return Optional.empty();
        }

        Map<Integer, List<PulsarPartitionSplit>> assignMap = new HashMap<>();
        for (Integer reader : readers) {
            Set<PulsarPartitionSplit> pendingSplits = sharedPendingPartitionSplits.remove(reader);
            if (pendingSplits == null) {
                pendingSplits = new HashSet<>();
            }

            Set<String> assignedSplits =
                    readerAssignedSplits.computeIfAbsent(reader, r -> new HashSet<>());

            for (TopicPartition partition : appendedPartitions) {
                String partitionName = partition.toString();
                if (!assignedSplits.contains(partitionName)) {
                    pendingSplits.add(new PulsarPartitionSplit(partition, stopCursor));
                    assignedSplits.add(partitionName);
                }
            }

            if (!pendingSplits.isEmpty()) {
                assignMap.put(reader, new ArrayList<>(pendingSplits));
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
        Set<PulsarPartitionSplit> pendingSplits = sharedPendingPartitionSplits.get(reader);
        Set<String> assignedSplits = readerAssignedSplits.get(reader);

        return !enablePartitionDiscovery
                && initialized
                && (pendingSplits == null || pendingSplits.isEmpty())
                && (assignedSplits != null && assignedSplits.size() == appendedPartitions.size());
    }

    @Override
    public PulsarSourceEnumState snapshotState() {
        return new PulsarSourceEnumState(
                appendedPartitions,
                new HashSet<>(),
                sharedPendingPartitionSplits,
                readerAssignedSplits,
                initialized);
    }
}
