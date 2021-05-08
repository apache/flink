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
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 */
public class TaskExecutorPartitionTrackerImpl
        extends AbstractPartitionTracker<JobID, TaskExecutorPartitionInfo>
        implements TaskExecutorPartitionTracker {

    private final Map<IntermediateDataSetID, DataSetEntry> clusterPartitions = new HashMap<>();
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;

    public TaskExecutorPartitionTrackerImpl(ShuffleEnvironment<?, ?> shuffleEnvironment) {
        this.shuffleEnvironment = shuffleEnvironment;
    }

    @Override
    public void startTrackingPartition(
            JobID producingJobId, TaskExecutorPartitionInfo partitionInfo) {
        Preconditions.checkNotNull(producingJobId);
        Preconditions.checkNotNull(partitionInfo);

        startTrackingPartition(producingJobId, partitionInfo.getResultPartitionId(), partitionInfo);
    }

    @Override
    public void stopTrackingAndReleaseJobPartitions(
            Collection<ResultPartitionID> partitionsToRelease) {
        if (partitionsToRelease.isEmpty()) {
            return;
        }

        stopTrackingPartitions(partitionsToRelease);
        shuffleEnvironment.releasePartitionsLocally(partitionsToRelease);
    }

    @Override
    public void stopTrackingAndReleaseJobPartitionsFor(JobID producingJobId) {
        Collection<ResultPartitionID> partitionsForJob =
                CollectionUtil.project(
                        stopTrackingPartitionsFor(producingJobId),
                        PartitionTrackerEntry::getResultPartitionId);
        shuffleEnvironment.releasePartitionsLocally(partitionsForJob);
    }

    @Override
    public void promoteJobPartitions(Collection<ResultPartitionID> partitionsToPromote) {
        if (partitionsToPromote.isEmpty()) {
            return;
        }

        final Collection<PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo>>
                partitionTrackerEntries = stopTrackingPartitions(partitionsToPromote);

        for (PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo> partitionTrackerEntry :
                partitionTrackerEntries) {
            final TaskExecutorPartitionInfo dataSetMetaInfo = partitionTrackerEntry.getMetaInfo();
            final DataSetEntry dataSetEntry =
                    clusterPartitions.computeIfAbsent(
                            dataSetMetaInfo.getIntermediateDataSetId(),
                            ignored -> new DataSetEntry(dataSetMetaInfo.getNumberOfPartitions()));
            dataSetEntry.addPartition(partitionTrackerEntry.getResultPartitionId());
        }
    }

    @Override
    public void stopTrackingAndReleaseClusterPartitions(
            Collection<IntermediateDataSetID> dataSetsToRelease) {
        for (IntermediateDataSetID dataSetID : dataSetsToRelease) {
            final DataSetEntry dataSetEntry = clusterPartitions.remove(dataSetID);
            final Set<ResultPartitionID> partitionIds = dataSetEntry.getPartitionIds();
            shuffleEnvironment.releasePartitionsLocally(partitionIds);
        }
    }

    @Override
    public void stopTrackingAndReleaseAllClusterPartitions() {
        clusterPartitions.values().stream()
                .map(DataSetEntry::getPartitionIds)
                .forEach(shuffleEnvironment::releasePartitionsLocally);
        clusterPartitions.clear();
    }

    @Override
    public ClusterPartitionReport createClusterPartitionReport() {
        List<ClusterPartitionReport.ClusterPartitionReportEntry> reportEntries =
                clusterPartitions.entrySet().stream()
                        .map(
                                entry ->
                                        new ClusterPartitionReport.ClusterPartitionReportEntry(
                                                entry.getKey(),
                                                entry.getValue().getPartitionIds(),
                                                entry.getValue().getTotalNumberOfPartitions()))
                        .collect(Collectors.toList());

        return new ClusterPartitionReport(reportEntries);
    }

    private static class DataSetEntry {

        private final Set<ResultPartitionID> partitionIds = new HashSet<>();
        private final int totalNumberOfPartitions;

        private DataSetEntry(int totalNumberOfPartitions) {
            this.totalNumberOfPartitions = totalNumberOfPartitions;
        }

        void addPartition(ResultPartitionID resultPartitionId) {
            partitionIds.add(resultPartitionId);
        }

        public Set<ResultPartitionID> getPartitionIds() {
            return partitionIds;
        }

        public int getTotalNumberOfPartitions() {
            return totalNumberOfPartitions;
        }
    }
}
