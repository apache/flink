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

package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe Utility for tracking partitions.
 */
@ThreadSafe
public class PartitionTable {

	private final Map<JobID, Set<ResultPartitionID>> trackedPartitionsPerJob = new ConcurrentHashMap<>(8);

	/**
	 * Returns whether any partitions are being tracked for the given job.
	 */
	public boolean hasTrackedPartitions(JobID jobId) {
		return trackedPartitionsPerJob.containsKey(jobId);
	}

	/**
	 * Starts the tracking of the given partition for the given job.
	 */
	public void startTrackingPartitions(JobID jobId, Collection<ResultPartitionID> newPartitionIds) {
		Preconditions.checkNotNull(jobId);
		Preconditions.checkNotNull(newPartitionIds);

		trackedPartitionsPerJob.compute(jobId, (ignored, partitionIds) -> {
			if (partitionIds == null) {
				partitionIds = new HashSet<>(8);
			}
			partitionIds.addAll(newPartitionIds);
			return partitionIds;
		});
	}

	/**
	 * Stops the tracking of all partition for the given job.
	 */
	public Collection<ResultPartitionID> stopTrackingPartitions(JobID jobId) {
		Preconditions.checkNotNull(jobId);

		Set<ResultPartitionID> storedPartitions = trackedPartitionsPerJob.remove(jobId);
		return storedPartitions == null
			? Collections.emptyList()
			: storedPartitions;
	}

	/**
	 * Stops the tracking of the given set of partitions for the given job.
	 */
	public void stopTrackingPartitions(JobID jobId, Collection<ResultPartitionID> partitionIds) {
		Preconditions.checkNotNull(jobId);
		Preconditions.checkNotNull(partitionIds);

		// If the JobID is unknown we do not fail here, in line with ShuffleEnvironment#releaseFinishedPartitions
		trackedPartitionsPerJob.computeIfPresent(
			jobId,
			(key, resultPartitionIDS) -> {
				resultPartitionIDS.removeAll(partitionIds);
				return resultPartitionIDS.isEmpty()
					? null
					: resultPartitionIDS;
			});
	}
}
