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
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Interface for components that manage cluster partitions. */
public interface ClusterPartitionManager {

    /**
     * Returns all datasets for which partitions are being tracked.
     *
     * @return tracked datasets
     */
    CompletableFuture<Map<IntermediateDataSetID, DataSetMetaInfo>> listDataSets();

    /**
     * Releases all partitions associated with the given dataset.
     *
     * @param dataSetToRelease dataset for which all associated partitions should be released
     * @return future that is completed once all partitions have been released
     */
    CompletableFuture<Void> releaseClusterPartitions(IntermediateDataSetID dataSetToRelease);

    /**
     * Report the cluster partitions status in the task executor.
     *
     * @param taskExecutorId The id of the task executor.
     * @param clusterPartitionReport The status of the cluster partitions.
     * @return future that is completed once the report have been processed.
     */
    CompletableFuture<Void> reportClusterPartitions(
            ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport);

    /**
     * Get the shuffle descriptors of the cluster partitions ordered by partition number.
     *
     * @param intermediateDataSetID The id of the dataset.
     * @return shuffle descriptors of the cluster partitions.
     */
    CompletableFuture<List<ShuffleDescriptor>> getClusterPartitionsShuffleDescriptors(
            IntermediateDataSetID intermediateDataSetID);
}
