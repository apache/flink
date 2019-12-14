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

import org.apache.flink.runtime.taskexecutor.partition.PartitionTable;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base partition tracker implementation, providing underlying data-structures for storing partitions, their associated
 * keys and meta-information.
 */
public abstract class AbstractPartitionTracker<K, M> implements PartitionTracker<K, M> {

	private final PartitionTable<K> partitionTable = new PartitionTable<>();
	private final Map<ResultPartitionID, PartitionInfo<K, M>> partitionInfos = new HashMap<>();

	void startTrackingPartition(K key, ResultPartitionID resultPartitionId, M metaInfo) {
		partitionInfos.put(resultPartitionId, new PartitionInfo<>(key, metaInfo));
		partitionTable.startTrackingPartitions(key, Collections.singletonList(resultPartitionId));
	}

	@Override
	public Collection<PartitionTrackerEntry<K, M>> stopTrackingPartitionsFor(K key) {
		Preconditions.checkNotNull(key);

		// this is a bit icky since we make 2 calls to pT#stopTrackingPartitions
		final Collection<ResultPartitionID> resultPartitionIds = partitionTable.stopTrackingPartitions(key);

		return stopTrackingPartitions(resultPartitionIds);
	}

	@Override
	public Collection<PartitionTrackerEntry<K, M>> stopTrackingPartitions(Collection<ResultPartitionID> resultPartitionIds) {
		Preconditions.checkNotNull(resultPartitionIds);

		return resultPartitionIds.stream()
			.map(this::internalStopTrackingPartition)
			.flatMap(AbstractPartitionTracker::asStream)
			.collect(Collectors.toList());
	}

	@Override
	public boolean isTrackingPartitionsFor(K key) {
		Preconditions.checkNotNull(key);

		return partitionTable.hasTrackedPartitions(key);
	}

	@Override
	public boolean isPartitionTracked(final ResultPartitionID resultPartitionID) {
		Preconditions.checkNotNull(resultPartitionID);

		return partitionInfos.containsKey(resultPartitionID);
	}

	private Optional<PartitionTrackerEntry<K, M>> internalStopTrackingPartition(ResultPartitionID resultPartitionId) {
		Preconditions.checkNotNull(resultPartitionId);

		final PartitionInfo<K, M> partitionInfo = partitionInfos.remove(resultPartitionId);
		if (partitionInfo == null) {
			return Optional.empty();
		}
		partitionTable.stopTrackingPartitions(partitionInfo.getKey(), Collections.singletonList(resultPartitionId));

		return Optional.of(new PartitionTrackerEntry<>(resultPartitionId, partitionInfo.key, partitionInfo.getMetaInfo()));
	}

	private static class PartitionInfo<K, M> {

		private final K key;
		private final M metaInfo;

		PartitionInfo(K key, M metaInfo) {
			this.key = key;
			this.metaInfo = metaInfo;
		}

		public K getKey() {
			return key;
		}

		public M getMetaInfo() {
			return metaInfo;
		}
	}

	private static <X> Stream<X> asStream(Optional<X> optional) {
		if (optional.isPresent()) {
			return Stream.of(optional.get());
		} else {
			return Stream.empty();
		}
	}
}
