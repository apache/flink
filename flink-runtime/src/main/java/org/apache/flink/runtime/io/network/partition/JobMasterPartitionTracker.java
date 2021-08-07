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

import java.util.Collection;

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
     * Releases the job partitions and promotes the cluster partitions, and stops the tracking of
     * partitions that were released/promoted.
     */
    void stopTrackingAndReleaseOrPromotePartitions(
            Collection<ResultPartitionID> resultPartitionIds);

    /** Get all the partitions under tracking. */
    Collection<ResultPartitionDeploymentDescriptor> getAllTrackedPartitions();
}
