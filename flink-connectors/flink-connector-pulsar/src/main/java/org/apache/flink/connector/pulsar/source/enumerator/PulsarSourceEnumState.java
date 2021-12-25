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

import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import java.util.Map;
import java.util.Set;

/**
 * The state class for pulsar source enumerator, used for storing the split state. This class is
 * managed and controlled by {@link SplitsAssignmentState}.
 */
public class PulsarSourceEnumState {

    /** The topic partitions that have been appended to this source. */
    private final Set<TopicPartition> appendedPartitions;

    /**
     * We convert the topic partition into a split and add to this pending list for assigning to a
     * reader. It is used for Key_Shared, Failover, Exclusive subscription.
     */
    private final Set<PulsarPartitionSplit> pendingPartitionSplits;

    /**
     * It is used for Shared subscription. When a reader is crashed in Shared subscription, its
     * splits would be put in here.
     */
    private final Map<Integer, Set<PulsarPartitionSplit>> sharedPendingPartitionSplits;

    /**
     * A {@link PulsarPartitionSplit} should be assigned for all flink readers. Using this map for
     * recording assign status.
     */
    private final Map<Integer, Set<String>> readerAssignedSplits;

    private final boolean initialized;

    public PulsarSourceEnumState(
            Set<TopicPartition> appendedPartitions,
            Set<PulsarPartitionSplit> pendingPartitionSplits,
            Map<Integer, Set<PulsarPartitionSplit>> pendingSharedPartitionSplits,
            Map<Integer, Set<String>> readerAssignedSplits,
            boolean initialized) {
        this.appendedPartitions = appendedPartitions;
        this.pendingPartitionSplits = pendingPartitionSplits;
        this.sharedPendingPartitionSplits = pendingSharedPartitionSplits;
        this.readerAssignedSplits = readerAssignedSplits;
        this.initialized = initialized;
    }

    public Set<TopicPartition> getAppendedPartitions() {
        return appendedPartitions;
    }

    public Set<PulsarPartitionSplit> getPendingPartitionSplits() {
        return pendingPartitionSplits;
    }

    public Map<Integer, Set<PulsarPartitionSplit>> getSharedPendingPartitionSplits() {
        return sharedPendingPartitionSplits;
    }

    public Map<Integer, Set<String>> getReaderAssignedSplits() {
        return readerAssignedSplits;
    }

    public boolean isInitialized() {
        return initialized;
    }
}
