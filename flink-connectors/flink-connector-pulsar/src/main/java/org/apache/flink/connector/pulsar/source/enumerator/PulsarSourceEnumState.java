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

import java.io.Serializable;
import java.util.Set;

/**
 * The state class for pulsar source enumerator, used for storing the split state. This class is
 * managed and controlled by {@link SplitAssignmentState}.
 */
public class PulsarSourceEnumState implements Serializable {
    private static final long serialVersionUID = -1228111807064171730L;

    private final Set<TopicPartition> appendedPartitions;

    private final Set<PulsarPartitionSplit> pendingPartitionSplits;

    private final boolean initialized;

    public PulsarSourceEnumState(
            Set<TopicPartition> appendedPartitions,
            Set<PulsarPartitionSplit> pendingPartitionSplits,
            boolean initialized) {
        this.appendedPartitions = appendedPartitions;
        this.pendingPartitionSplits = pendingPartitionSplits;
        this.initialized = initialized;
    }

    public Set<TopicPartition> getAppendedPartitions() {
        return appendedPartitions;
    }

    public Set<PulsarPartitionSplit> getPendingPartitionSplits() {
        return pendingPartitionSplits;
    }

    public boolean isInitialized() {
        return initialized;
    }
}
