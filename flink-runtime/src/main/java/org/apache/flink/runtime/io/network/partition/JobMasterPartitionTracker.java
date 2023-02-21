/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 */
public interface JobMasterPartitionTracker
        extends PartitionTracker<ResourceID, ResultPartitionDeploymentDescriptor> {

    /**
     * Starts the tracking of the given partition for the given task executor ID.
     *
     * @param producingTaskExecutorId ID of task executor on which the partition is produced
     * @param resultPartitionDeploymentDescriptor deployment descriptor of the partition
     */
    void startTrackingPartition(
            ResourceID producingTaskExecutorId,
            ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor);

    /** Releases the given partitions and stop the tracking of partitions that were released. */
    default void stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> resultPartitionIds) {
        stopTrackingAndReleasePartitions(resultPartitionIds, true);
    }

    /**
     * Releases the given partitions and stop the tracking of partitions that were released. The
     * boolean flag indicates whether we need to notify the ShuffleMaster to release all external
     * resources or not.
     */
    void stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> resultPartitionIds, boolean releaseOnShuffleMaster);

    /**
     * Promotes the given partitions, and stops the tracking of partitions that were promoted.
     *
     * @param resultPartitionIds ID of the partition containing both job partitions and cluster
     *     partitions.
     * @return Future that will be completed if the partitions are promoted.
     */
    CompletableFuture<Void> stopTrackingAndPromotePartitions(
            Collection<ResultPartitionID> resultPartitionIds);

    /** Gets all the partitions under tracking. */
    Collection<ResultPartitionDeploymentDescriptor> getAllTrackedPartitions();

    /** Gets all the non-cluster partitions under tracking. */
    default Collection<ResultPartitionDeploymentDescriptor> getAllTrackedNonClusterPartitions() {
        return getAllTrackedPartitions().stream()
                .filter(descriptor -> !descriptor.getPartitionType().isPersistent())
                .collect(Collectors.toList());
    }

    /** Gets all the cluster partitions under tracking. */
    default Collection<ResultPartitionDeploymentDescriptor> getAllTrackedClusterPartitions() {
        return getAllTrackedPartitions().stream()
                .filter(descriptor -> descriptor.getPartitionType().isPersistent())
                .collect(Collectors.toList());
    }

    void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

    /** Get the shuffle descriptors of the cluster partitions ordered by partition number. */
    List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID);
}
