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
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 */
public class TaskExecutorPartitionTrackerImpl extends AbstractPartitionTracker<JobID, TaskExecutorPartitionInfo> implements TaskExecutorPartitionTracker {

	private final Map<TaskExecutorPartitionInfo, Set<ResultPartitionID>> clusterPartitions = new HashMap<>();
	private final ShuffleEnvironment<?, ?> shuffleEnvironment;

	public TaskExecutorPartitionTrackerImpl(ShuffleEnvironment<?, ?> shuffleEnvironment) {
		this.shuffleEnvironment = shuffleEnvironment;
	}

	@Override
	public void startTrackingPartition(JobID producingJobId, TaskExecutorPartitionInfo partitionInfo) {
		Preconditions.checkNotNull(producingJobId);
		Preconditions.checkNotNull(partitionInfo);

		startTrackingPartition(producingJobId, partitionInfo.getResultPartitionId(), partitionInfo);
	}

	@Override
	public void stopTrackingAndReleaseJobPartitions(Collection<ResultPartitionID> partitionsToRelease) {
		stopTrackingPartitions(partitionsToRelease);
		shuffleEnvironment.releasePartitionsLocally(partitionsToRelease);
	}

	@Override
	public void stopTrackingAndReleaseJobPartitionsFor(JobID producingJobId) {
		Collection<ResultPartitionID> partitionsForJob = CollectionUtil.project(
			stopTrackingPartitionsFor(producingJobId),
			PartitionTrackerEntry::getResultPartitionId);
		shuffleEnvironment.releasePartitionsLocally(partitionsForJob);
	}

	@Override
	public void promoteJobPartitions(Collection<ResultPartitionID> partitionsToPromote) {
		final Collection<PartitionTrackerEntry<JobID, TaskExecutorPartitionInfo>> partitionTrackerEntries = stopTrackingPartitions(partitionsToPromote);

		final Map<TaskExecutorPartitionInfo, Set<ResultPartitionID>> newClusterPartitions = partitionTrackerEntries.stream()
			.collect(Collectors.groupingBy(
				PartitionTrackerEntry::getMetaInfo,
				Collectors.mapping(PartitionTrackerEntry::getResultPartitionId, Collectors.toSet())));

		newClusterPartitions.forEach(
			(dataSetMetaInfo, newPartitionEntries) -> clusterPartitions.compute(dataSetMetaInfo, (ignored, existingPartitions) -> {
				if (existingPartitions == null) {
					return newPartitionEntries;
				} else {
					existingPartitions.addAll(newPartitionEntries);
					return existingPartitions;
				}
			}));
	}

	@Override
	public void stopTrackingAndReleaseAllClusterPartitions() {
		clusterPartitions.values().forEach(shuffleEnvironment::releasePartitionsLocally);
		clusterPartitions.clear();
	}

	@Override
	public ClusterPartitionReport createClusterPartitionReport() {
		List<ClusterPartitionReport.ClusterPartitionReportEntry> collect = clusterPartitions.entrySet().stream().map(entry -> {
			TaskExecutorPartitionInfo dataSetMetaInfo = entry.getKey();
			Set<ResultPartitionID> partitionsIds = entry.getValue();
			return new ClusterPartitionReport.ClusterPartitionReportEntry(
				dataSetMetaInfo.getIntermediateDataSetId(),
				partitionsIds,
				dataSetMetaInfo.getNumberOfPartitions());
		}).collect(Collectors.toList());

		return new ClusterPartitionReport(collect);
	}
}
