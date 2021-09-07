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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.api.SubscriptionType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** The state class for recording the split assignment. */
@Internal
public class SplitsAssignmentState {

    private final StopCursor stopCursor;
    private final SourceConfiguration sourceConfiguration;

    // The dynamic states for checkpoint.
    private final Set<TopicPartition> appendedPartitions;
    // This pending splits is used for Key_Shared, Failover, Exclusive subscription.
    private final Set<PulsarPartitionSplit> pendingPartitionSplits;
    // These two fields are used for Shared subscription.
    private final Map<Integer, Set<PulsarPartitionSplit>> sharedPendingPartitionSplits;
    private final Map<Integer, Set<String>> readerAssignedSplits;
    private boolean initialized;

    public SplitsAssignmentState(StopCursor stopCursor, SourceConfiguration sourceConfiguration) {
        this.stopCursor = stopCursor;
        this.sourceConfiguration = sourceConfiguration;
        this.appendedPartitions = new HashSet<>();
        this.pendingPartitionSplits = new HashSet<>();
        this.sharedPendingPartitionSplits = new HashMap<>();
        this.readerAssignedSplits = new HashMap<>();
        this.initialized = false;
    }

    public SplitsAssignmentState(
            StopCursor stopCursor,
            SourceConfiguration sourceConfiguration,
            PulsarSourceEnumState sourceEnumState) {
        this.stopCursor = stopCursor;
        this.sourceConfiguration = sourceConfiguration;
        this.appendedPartitions = sourceEnumState.getAppendedPartitions();
        this.pendingPartitionSplits = sourceEnumState.getPendingPartitionSplits();
        this.sharedPendingPartitionSplits = sourceEnumState.getSharedPendingPartitionSplits();
        this.readerAssignedSplits = sourceEnumState.getReaderAssignedSplits();
        this.initialized = sourceEnumState.isInitialized();
    }

    public PulsarSourceEnumState snapshotState() {
        return new PulsarSourceEnumState(
                appendedPartitions,
                pendingPartitionSplits,
                sharedPendingPartitionSplits,
                readerAssignedSplits,
                initialized);
    }

    /**
     * Append the new fetched partitions to current state. We would generate pending source split
     * for downstream pulsar readers. Since the {@link SplitEnumeratorContext} don't support put the
     * split back to enumerator, we don't support partition deletion.
     *
     * @param fetchedPartitions The partitions from the {@link PulsarSubscriber}.
     */
    public void appendTopicPartitions(Set<TopicPartition> fetchedPartitions) {
        for (TopicPartition partition : fetchedPartitions) {
            // If this partition is a new partition.
            if (!appendedPartitions.contains(partition)) {
                if (!sharePartition()) {
                    // Create a split and add it to pending list.
                    pendingPartitionSplits.add(createSplit(partition));
                }

                // Shared subscription don't create splits, we just register partitions.
                appendedPartitions.add(partition);
            }
        }

        // Update this initialize flag.
        if (!initialized) {
            this.initialized = true;
        }
    }

    public boolean containsTopic(String topicName) {
        return appendedPartitions.stream()
                .anyMatch(partition -> Objects.equals(partition.getFullTopicName(), topicName));
    }

    /** Put these splits back to pending list. */
    public void putSplitsBackToPendingList(List<PulsarPartitionSplit> splits, int readerId) {
        if (!sharePartition()) {
            // Put these splits back to normal pending list.
            pendingPartitionSplits.addAll(splits);
        } else {
            // Put the splits back to shared pending list.
            Set<PulsarPartitionSplit> pending =
                    sharedPendingPartitionSplits.computeIfAbsent(readerId, id -> new HashSet<>());
            pending.addAll(splits);
        }
    }

    public Optional<SplitsAssignment<PulsarPartitionSplit>> assignSplits(
            List<Integer> pendingReaders) {
        // Avoid empty readers assign.
        if (pendingReaders.isEmpty()) {
            return Optional.empty();
        }

        Map<Integer, List<PulsarPartitionSplit>> assignMap;

        // We extract the assign logic into two method for better readability.
        if (!sharePartition()) {
            assignMap = assignNormalSplits(pendingReaders);
        } else {
            assignMap = assignSharedSplits(pendingReaders);
        }

        if (assignMap.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new SplitsAssignment<>(assignMap));
        }
    }

    /**
     * @return It would return true only if periodically partition discovery is disabled, the
     *     initializing partition discovery has finished AND there is no pending splits for
     *     assignment.
     */
    public boolean noMoreNewPartitionSplits() {
        return !sourceConfiguration.enablePartitionDiscovery()
                && initialized
                && pendingPartitionSplits.isEmpty();
    }

    // ----------------- private methods -------------------

    /** The splits don't shared for all the readers. */
    private Map<Integer, List<PulsarPartitionSplit>> assignNormalSplits(
            List<Integer> pendingReaders) {
        Map<Integer, List<PulsarPartitionSplit>> assignMap = new HashMap<>();

        // Drain a list of splits.
        List<PulsarPartitionSplit> pendingSplits = drainPendingPartitionsSplits();
        for (int i = 0; i < pendingSplits.size(); i++) {
            PulsarPartitionSplit split = pendingSplits.get(i);
            int readerId = pendingReaders.get(i % pendingReaders.size());
            assignMap.computeIfAbsent(readerId, id -> new ArrayList<>()).add(split);
        }

        return assignMap;
    }

    /** Every split would be shared among available readers. */
    private Map<Integer, List<PulsarPartitionSplit>> assignSharedSplits(
            List<Integer> pendingReaders) {
        Map<Integer, List<PulsarPartitionSplit>> assignMap = new HashMap<>();

        // Drain the splits from share pending list.
        for (Integer reader : pendingReaders) {
            Set<PulsarPartitionSplit> pendingSplits = sharedPendingPartitionSplits.remove(reader);
            if (pendingSplits == null) {
                pendingSplits = new HashSet<>();
            }

            Set<String> assignedSplits =
                    readerAssignedSplits.computeIfAbsent(reader, r -> new HashSet<>());

            for (TopicPartition partition : appendedPartitions) {
                String partitionName = partition.toString();
                if (!assignedSplits.contains(partitionName)) {
                    pendingSplits.add(createSplit(partition));
                    assignedSplits.add(partitionName);
                }
            }

            if (!pendingSplits.isEmpty()) {
                assignMap.put(reader, new ArrayList<>(pendingSplits));
            }
        }

        return assignMap;
    }

    private PulsarPartitionSplit createSplit(TopicPartition partition) {
        try {
            StopCursor stop = InstantiationUtil.clone(stopCursor);
            return new PulsarPartitionSplit(partition, stop);
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<PulsarPartitionSplit> drainPendingPartitionsSplits() {
        List<PulsarPartitionSplit> splits = new ArrayList<>(pendingPartitionSplits);
        pendingPartitionSplits.clear();

        return splits;
    }

    /** {@link SubscriptionType#Shared} mode should share a same split for all the readers. */
    private boolean sharePartition() {
        return sourceConfiguration.getSubscriptionType() == SubscriptionType.Shared;
    }
}
