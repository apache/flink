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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * No-op {@link ResourceManagerPartitionTracker} implementation; does not track any partition and
 * never regards a data set corrupted.
 */
public enum NoOpResourceManagerPartitionTracker implements ResourceManagerPartitionTracker {
    INSTANCE;

    @Override
    public void processTaskExecutorClusterPartitionReport(
            ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport) {}

    @Override
    public void processTaskExecutorShutdown(ResourceID taskExecutorId) {}

    @Override
    public CompletableFuture<Void> releaseClusterPartitions(IntermediateDataSetID dataSetId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Map<IntermediateDataSetID, DataSetMetaInfo> listDataSets() {
        return Collections.emptyMap();
    }

    @SuppressWarnings(
            "unused") // unused parameter allows usage as a ResourceManagerPartitionTrackerFactory
    public static ResourceManagerPartitionTracker get(
            TaskExecutorClusterPartitionReleaser ignored) {
        return INSTANCE;
    }
}
