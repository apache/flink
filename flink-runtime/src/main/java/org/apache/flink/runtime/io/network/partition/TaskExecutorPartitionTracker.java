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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;

import java.util.Collection;

/** Utility for tracking partitions. */
public interface TaskExecutorPartitionTracker
        extends PartitionTracker<JobID, TaskExecutorPartitionInfo> {

    /**
     * Starts the tracking of the given partition for the given job.
     *
     * @param producingJobId ID of job by which the partition is produced
     * @param partitionInfo information about the partition
     */
    void startTrackingPartition(JobID producingJobId, TaskExecutorPartitionInfo partitionInfo);

    /** Releases the given partitions and stop the tracking of partitions that were released. */
    void stopTrackingAndReleaseJobPartitions(Collection<ResultPartitionID> resultPartitionIds);

    /**
     * Releases all partitions for the given job and stop the tracking of partitions that were
     * released.
     */
    void stopTrackingAndReleaseJobPartitionsFor(JobID producingJobId);

    /** Promotes the given partitions. */
    void promoteJobPartitions(Collection<ResultPartitionID> partitionsToPromote);

    /**
     * Releases partitions associated with the given datasets and stops tracking of partitions that
     * were released.
     *
     * @param dataSetsToRelease data sets to release
     */
    void stopTrackingAndReleaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease);

    /** Releases and stops tracking all partitions. */
    void stopTrackingAndReleaseAllClusterPartitions();

    /**
     * Creates a {@link ClusterPartitionReport}, describing which cluster partitions are currently
     * available.
     */
    ClusterPartitionReport createClusterPartitionReport();
}
