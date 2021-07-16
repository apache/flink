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
import org.apache.flink.connector.pulsar.source.config.PulsarSourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/** The state class for recording the split assignment. */
@Internal
public class SplitAssignmentState implements Serializable {
    private static final long serialVersionUID = -3244274150389546170L;

    private final Supplier<StartCursor> startCursorSupplier;
    private final Supplier<StopCursor> stopCursorSupplier;
    private final PulsarSourceConfiguration sourceConfiguration;

    private final Set<TopicPartition> appendedPartitions;

    private final Set<PulsarPartitionSplit> pendingPartitionSplits;

    private boolean initialized;

    public SplitAssignmentState(
            Supplier<StartCursor> startCursorSupplier,
            Supplier<StopCursor> stopCursorSupplier,
            PulsarSourceConfiguration sourceConfiguration) {
        this.startCursorSupplier = startCursorSupplier;
        this.stopCursorSupplier = stopCursorSupplier;
        this.sourceConfiguration = sourceConfiguration;

        this.appendedPartitions = new HashSet<>();
        this.pendingPartitionSplits = new HashSet<>();
        this.initialized = false;
    }

    public SplitAssignmentState(
            Supplier<StartCursor> startCursorSupplier,
            Supplier<StopCursor> stopCursorSupplier,
            PulsarSourceConfiguration sourceConfiguration,
            PulsarSourceEnumState sourceEnumState) {
        this.startCursorSupplier = startCursorSupplier;
        this.stopCursorSupplier = stopCursorSupplier;
        this.sourceConfiguration = sourceConfiguration;

        this.appendedPartitions = sourceEnumState.getAppendedPartitions();
        this.pendingPartitionSplits = sourceEnumState.getPendingPartitionSplits();
        this.initialized = sourceEnumState.isInitialized();
    }

    public PulsarSourceEnumState snapshotState() {
        return new PulsarSourceEnumState(appendedPartitions, pendingPartitionSplits, initialized);
    }

    /**
     * Append the new fetched partitions to current state. We would generate pending source split
     * for downstream pulsar readers. Since the {@link SplitEnumeratorContext} don't support put the
     * split back to enumerator, we don't support partition deletion.
     *
     * @param fetchedPartitions The partitions from the {@link PulsarSubscriber}.
     */
    public void appendTopicPartitions(Set<TopicPartition> fetchedPartitions) {
        for (TopicPartition fetchedPartition : fetchedPartitions) {
            if (!appendedPartitions.contains(fetchedPartition)) {
                // This is a new topic partition.
                PulsarPartitionSplit split =
                        new PulsarPartitionSplit(
                                fetchedPartition,
                                startCursorSupplier.get(),
                                stopCursorSupplier.get());

                pendingPartitionSplits.add(split);
                appendedPartitions.add(fetchedPartition);
            }
        }

        // Update this initialize flag.
        if (!initialized) {
            this.initialized = true;
        }
    }

    /** Put these splits to pending list. */
    public void addPendingPartitionSplits(List<PulsarPartitionSplit> splits) {
        pendingPartitionSplits.addAll(splits);
    }

    public boolean havePendingPartitionSplits() {
        return !pendingPartitionSplits.isEmpty();
    }

    public List<PulsarPartitionSplit> drainPendingPartitionsSplits() {
        List<PulsarPartitionSplit> splits = new ArrayList<>(pendingPartitionSplits);
        splits.forEach(pendingPartitionSplits::remove);

        return splits;
    }

    /**
     * @return It would return true only if periodically partition discovery is disabled, the
     *     initializing partition discovery has finished AND there is no pending splits for
     *     assignment.
     */
    public boolean noMoreNewPartitionSplits() {
        return !sourceConfiguration.enablePartitionDiscovery()
                && initialized
                && !havePendingPartitionSplits();
    }
}
