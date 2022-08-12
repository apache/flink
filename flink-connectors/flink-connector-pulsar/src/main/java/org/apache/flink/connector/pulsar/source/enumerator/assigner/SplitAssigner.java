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
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The split assigner for different subscription. We would spread all the splits to different
 * readers and store all the state into checkpoint.
 */
@Internal
public interface SplitAssigner extends Serializable {

    /**
     * Add the current available partitions into assigner.
     *
     * @param fetchedPartitions The available partitions queried from Pulsar broker.
     * @return New topic partitions compare to previous registered partitions.
     */
    List<TopicPartition> registerTopicPartitions(Set<TopicPartition> fetchedPartitions);

    /**
     * Add a split back to the split assigner if the reader fails. We would try to reassign the
     * split or add it to the pending list.
     */
    void addSplitsBack(List<PulsarPartitionSplit> splits, int subtaskId);

    /** Create a split assignment from the current readers. */
    Optional<SplitsAssignment<PulsarPartitionSplit>> createAssignment(List<Integer> readers);

    /**
     * It would return true only if periodically partition discovery is disabled, the initializing
     * partition discovery has finished AND there is no pending splits for assignment.
     */
    boolean noMoreSplits(Integer reader);

    /** Snapshot the current assign state into checkpoint. */
    PulsarSourceEnumState snapshotState();
}
