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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Default {@link ResourceManagerPartitionTracker} implementation.
 *
 * <p>Internal tracking info must only be updated upon reception of a {@link
 * ClusterPartitionReport}, as the task executor state is the source of truth.
 */
public class ResourceManagerPartitionTrackerImpl implements ResourceManagerPartitionTracker {

    private static final Logger LOG =
            LoggerFactory.getLogger(ResourceManagerPartitionTrackerImpl.class);

    private final Map<ResourceID, Set<IntermediateDataSetID>> taskExecutorToDataSets =
            new HashMap<>();
    private final Map<IntermediateDataSetID, Map<ResourceID, Set<ResultPartitionID>>>
            dataSetToTaskExecutors = new HashMap<>();
    private final Map<IntermediateDataSetID, DataSetMetaInfo> dataSetMetaInfo = new HashMap<>();
    private final Map<IntermediateDataSetID, CompletableFuture<Void>>
            partitionReleaseCompletionFutures = new HashMap<>();

    private final TaskExecutorClusterPartitionReleaser taskExecutorClusterPartitionReleaser;

    public ResourceManagerPartitionTrackerImpl(
            TaskExecutorClusterPartitionReleaser taskExecutorClusterPartitionReleaser) {
        this.taskExecutorClusterPartitionReleaser = taskExecutorClusterPartitionReleaser;
    }

    @Override
    public void processTaskExecutorClusterPartitionReport(
            ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport) {
        Preconditions.checkNotNull(taskExecutorId);
        Preconditions.checkNotNull(clusterPartitionReport);
        LOG.debug(
                "Processing cluster partition report from task executor {}: {}.",
                taskExecutorId,
                clusterPartitionReport);

        internalProcessClusterPartitionReport(taskExecutorId, clusterPartitionReport);
    }

    @Override
    public void processTaskExecutorShutdown(ResourceID taskExecutorId) {
        Preconditions.checkNotNull(taskExecutorId);
        LOG.debug("Processing shutdown of task executor {}.", taskExecutorId);

        internalProcessClusterPartitionReport(
                taskExecutorId, new ClusterPartitionReport(Collections.emptyList()));
    }

    @Override
    public CompletableFuture<Void> releaseClusterPartitions(IntermediateDataSetID dataSetId) {
        Preconditions.checkNotNull(dataSetId);
        if (!dataSetMetaInfo.containsKey(dataSetId)) {
            LOG.debug("Attempted released of unknown data set {}.", dataSetId);
            return CompletableFuture.completedFuture(null);
        }
        LOG.debug("Releasing cluster partitions for data set {}.", dataSetId);

        CompletableFuture<Void> partitionReleaseCompletionFuture =
                partitionReleaseCompletionFutures.computeIfAbsent(
                        dataSetId, ignored -> new CompletableFuture<>());
        internalReleasePartitions(Collections.singleton(dataSetId));
        return partitionReleaseCompletionFuture;
    }

    private void internalProcessClusterPartitionReport(
            ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport) {
        final Set<IntermediateDataSetID> dataSetsWithLostPartitions =
                clusterPartitionReport.getEntries().isEmpty()
                        ? processEmptyReport(taskExecutorId)
                        : setHostedDataSetsAndCheckCorruption(
                                taskExecutorId, clusterPartitionReport.getEntries());

        updateDataSetMetaData(clusterPartitionReport);

        checkForFullyLostDatasets(dataSetsWithLostPartitions);

        internalReleasePartitions(dataSetsWithLostPartitions);
    }

    private void internalReleasePartitions(Set<IntermediateDataSetID> dataSetsToRelease) {
        Map<ResourceID, Set<IntermediateDataSetID>> releaseCalls =
                prepareReleaseCalls(dataSetsToRelease);
        releaseCalls.forEach(taskExecutorClusterPartitionReleaser::releaseClusterPartitions);
    }

    private Set<IntermediateDataSetID> processEmptyReport(ResourceID taskExecutorId) {
        Set<IntermediateDataSetID> previouslyHostedDatasets =
                taskExecutorToDataSets.remove(taskExecutorId);
        if (previouslyHostedDatasets == null) {
            // default path for task executors that never have any cluster partitions
            previouslyHostedDatasets = Collections.emptySet();
        } else {
            previouslyHostedDatasets.forEach(
                    dataSetId -> removeInnerKey(dataSetId, taskExecutorId, dataSetToTaskExecutors));
        }
        return previouslyHostedDatasets;
    }

    /**
     * Updates the data sets for which the given task executor is hosting partitions and returns
     * data sets that were corrupted due to a loss of partitions.
     *
     * @param taskExecutorId ID of the hosting TaskExecutor
     * @param reportEntries IDs of data sets for which partitions are hosted
     * @return corrupted data sets
     */
    private Set<IntermediateDataSetID> setHostedDataSetsAndCheckCorruption(
            ResourceID taskExecutorId,
            Collection<ClusterPartitionReport.ClusterPartitionReportEntry> reportEntries) {
        final Set<IntermediateDataSetID> currentlyHostedDatasets =
                reportEntries.stream()
                        .map(ClusterPartitionReport.ClusterPartitionReportEntry::getDataSetId)
                        .collect(Collectors.toSet());

        final Set<IntermediateDataSetID> previouslyHostedDataSets =
                taskExecutorToDataSets.put(taskExecutorId, currentlyHostedDatasets);

        // previously tracked data sets may be corrupted since we may be tracking less partitions
        // than before
        final Set<IntermediateDataSetID> potentiallyCorruptedDataSets =
                Optional.ofNullable(previouslyHostedDataSets).orElse(new HashSet<>(0));

        // update data set -> task executor mapping and find datasets for which lost a partition
        reportEntries.forEach(
                hostedPartition -> {
                    final Map<ResourceID, Set<ResultPartitionID>> taskExecutorHosts =
                            dataSetToTaskExecutors.computeIfAbsent(
                                    hostedPartition.getDataSetId(), ignored -> new HashMap<>());
                    final Set<ResultPartitionID> previouslyHostedPartitions =
                            taskExecutorHosts.put(
                                    taskExecutorId, hostedPartition.getHostedPartitions());

                    final boolean noPartitionLost =
                            previouslyHostedPartitions == null
                                    || hostedPartition
                                            .getHostedPartitions()
                                            .containsAll(previouslyHostedPartitions);
                    if (noPartitionLost) {
                        potentiallyCorruptedDataSets.remove(hostedPartition.getDataSetId());
                    }
                });

        // now only contains data sets for which a partition is no longer tracked
        return potentiallyCorruptedDataSets;
    }

    private void updateDataSetMetaData(ClusterPartitionReport clusterPartitionReport) {
        // add meta info for new data sets
        clusterPartitionReport
                .getEntries()
                .forEach(
                        entry ->
                                dataSetMetaInfo.compute(
                                        entry.getDataSetId(),
                                        (dataSetID, dataSetMetaInfo) -> {
                                            if (dataSetMetaInfo == null) {
                                                return DataSetMetaInfo
                                                        .withoutNumRegisteredPartitions(
                                                                entry.getNumTotalPartitions());
                                            } else {
                                                // double check that the meta data is consistent
                                                Preconditions.checkState(
                                                        dataSetMetaInfo.getNumTotalPartitions()
                                                                == entry.getNumTotalPartitions());
                                                return dataSetMetaInfo;
                                            }
                                        }));
    }

    private void checkForFullyLostDatasets(Set<IntermediateDataSetID> dataSetsWithLostPartitions) {
        dataSetsWithLostPartitions.forEach(
                dataSetId -> {
                    if (getHostingTaskExecutors(dataSetId).isEmpty()) {
                        LOG.debug(
                                "There are no longer partitions being tracked for dataset {}.",
                                dataSetId);
                        dataSetMetaInfo.remove(dataSetId);
                        Optional.ofNullable(partitionReleaseCompletionFutures.remove(dataSetId))
                                .map(future -> future.complete(null));
                    }
                });
    }

    private Map<ResourceID, Set<IntermediateDataSetID>> prepareReleaseCalls(
            Set<IntermediateDataSetID> dataSetsToRelease) {
        final Map<ResourceID, Set<IntermediateDataSetID>> releaseCalls = new HashMap<>();
        dataSetsToRelease.forEach(
                dataSetToRelease -> {
                    final Set<ResourceID> hostingTaskExecutors =
                            getHostingTaskExecutors(dataSetToRelease);
                    hostingTaskExecutors.forEach(
                            hostingTaskExecutor ->
                                    insert(hostingTaskExecutor, dataSetToRelease, releaseCalls));
                });
        return releaseCalls;
    }

    private Set<ResourceID> getHostingTaskExecutors(IntermediateDataSetID dataSetId) {
        Preconditions.checkNotNull(dataSetId);

        Map<ResourceID, Set<ResultPartitionID>> trackedPartitions =
                dataSetToTaskExecutors.get(dataSetId);
        if (trackedPartitions == null) {
            return Collections.emptySet();
        } else {
            return trackedPartitions.keySet();
        }
    }

    @Override
    public Map<IntermediateDataSetID, DataSetMetaInfo> listDataSets() {
        return dataSetMetaInfo.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    final Map<ResourceID, Set<ResultPartitionID>>
                                            taskExecutorToPartitions =
                                                    dataSetToTaskExecutors.get(entry.getKey());
                                    Preconditions.checkState(
                                            taskExecutorToPartitions != null,
                                            "Have metadata entry for dataset %s, but no partition is tracked.",
                                            entry.getKey());

                                    int numTrackedPartitions = 0;
                                    for (Set<ResultPartitionID> hostedPartitions :
                                            taskExecutorToPartitions.values()) {
                                        numTrackedPartitions += hostedPartitions.size();
                                    }

                                    return DataSetMetaInfo.withNumRegisteredPartitions(
                                            numTrackedPartitions,
                                            entry.getValue().getNumTotalPartitions());
                                }));
    }

    /**
     * Returns whether all maps are empty; used for checking for resource leaks in case entries
     * aren't properly removed.
     *
     * @return whether all contained maps are empty
     */
    @VisibleForTesting
    boolean areAllMapsEmpty() {
        return taskExecutorToDataSets.isEmpty()
                && dataSetToTaskExecutors.isEmpty()
                && dataSetMetaInfo.isEmpty()
                && partitionReleaseCompletionFutures.isEmpty();
    }

    private static <K, V> void insert(K key1, V value, Map<K, Set<V>> collection) {
        collection.compute(
                key1,
                (key, values) -> {
                    if (values == null) {
                        values = new HashSet<>();
                    }
                    values.add(value);
                    return values;
                });
    }

    private static <K1, K2, V> void removeInnerKey(
            K1 key1, K2 value, Map<K1, Map<K2, V>> collection) {
        collection.computeIfPresent(
                key1,
                (key, values) -> {
                    values.remove(value);
                    if (values.isEmpty()) {
                        return null;
                    }
                    return values;
                });
    }
}
