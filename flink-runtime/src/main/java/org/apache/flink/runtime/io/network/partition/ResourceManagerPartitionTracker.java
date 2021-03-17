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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Utility for tracking and releasing partitions on the ResourceManager. */
public interface ResourceManagerPartitionTracker {

    /**
     * Processes {@link ClusterPartitionReport} of a task executor. Updates the tracking information
     * for the respective task executor. Any partition no longer being hosted on the task executor
     * is considered lost, corrupting the corresponding data set. For any such data set this method
     * issues partition release calls to all task executors that are hosting partitions of this data
     * set.
     *
     * @param taskExecutorId origin of the report
     * @param clusterPartitionReport partition report
     */
    void processTaskExecutorClusterPartitionReport(
            ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport);

    /**
     * Processes the shutdown of task executor. Removes all tracking information for the given
     * executor, determines datasets that may be corrupted by the shutdown (and implied loss of
     * partitions). For any such data set this method issues partition release calls to all task
     * executors that are hosting partitions of this data set, and issues release calls.
     *
     * @param taskExecutorId task executor that shut down
     */
    void processTaskExecutorShutdown(ResourceID taskExecutorId);

    /**
     * Issues a release calls to all task executors that are hosting partitions of the given data
     * set.
     *
     * @param dataSetId data set to release
     */
    CompletableFuture<Void> releaseClusterPartitions(IntermediateDataSetID dataSetId);

    /**
     * Returns all data sets for which partitions are being tracked.
     *
     * @return tracked datasets
     */
    Map<IntermediateDataSetID, DataSetMetaInfo> listDataSets();
}
