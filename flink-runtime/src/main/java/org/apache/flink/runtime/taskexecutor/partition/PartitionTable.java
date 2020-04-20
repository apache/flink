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
public class PartitionTable<K> {

	private final Map<K, Set<ResultPartitionID>> trackedPartitionsPerKey = new ConcurrentHashMap<>(8);

	/**
	 * Returns whether any partitions are being tracked for the given key.
	 */
	public boolean hasTrackedPartitions(K key) {
		return trackedPartitionsPerKey.containsKey(key);
	}

	/**
	 * Starts the tracking of the given partition for the given key.
	 */
	public void startTrackingPartitions(K key, Collection<ResultPartitionID> newPartitionIds) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(newPartitionIds);

		if (newPartitionIds.isEmpty()) {
			return;
		}

		trackedPartitionsPerKey.compute(key, (ignored, partitionIds) -> {
			if (partitionIds == null) {
				partitionIds = new HashSet<>(8);
			}
			partitionIds.addAll(newPartitionIds);
			return partitionIds;
		});
	}

	/**
	 * Stops the tracking of all partition for the given key.
	 */
	public Collection<ResultPartitionID> stopTrackingPartitions(K key) {
		Preconditions.checkNotNull(key);

		Set<ResultPartitionID> storedPartitions = trackedPartitionsPerKey.remove(key);
		return storedPartitions == null
			? Collections.emptyList()
			: storedPartitions;
	}

	/**
	 * Stops the tracking of the given set of partitions for the given key.
	 */
	public void stopTrackingPartitions(K key, Collection<ResultPartitionID> partitionIds) {
		Preconditions.checkNotNull(key);
		Preconditions.checkNotNull(partitionIds);

		// If the key is unknown we do not fail here, in line with ShuffleEnvironment#releaseFinishedPartitions
		trackedPartitionsPerKey.computeIfPresent(
			key,
			(ignored, resultPartitionIDS) -> {
				resultPartitionIDS.removeAll(partitionIds);
				return resultPartitionIDS.isEmpty()
					? null
					: resultPartitionIDS;
			});
	}
}
