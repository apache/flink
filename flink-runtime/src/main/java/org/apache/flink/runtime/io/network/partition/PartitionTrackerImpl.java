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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskexecutor.partition.PartitionTable;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 */
public class PartitionTrackerImpl implements PartitionTracker {

	private final JobID jobId;

	private final PartitionTable<ResourceID> partitionTable = new PartitionTable<>();
	private final Map<ResultPartitionID, PartitionInfo> partitionInfos = new HashMap<>();

	private final ShuffleMaster<?> shuffleMaster;

	private final PartitionTrackerFactory.TaskExecutorGatewayLookup taskExecutorGatewayLookup;

	public PartitionTrackerImpl(
		JobID jobId,
		ShuffleMaster<?> shuffleMaster,
		PartitionTrackerFactory.TaskExecutorGatewayLookup taskExecutorGatewayLookup) {

		this.jobId = Preconditions.checkNotNull(jobId);
		this.shuffleMaster = Preconditions.checkNotNull(shuffleMaster);
		this.taskExecutorGatewayLookup = taskExecutorGatewayLookup;
	}

	@Override
	public void startTrackingPartition(ResourceID producingTaskExecutorId, ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
		Preconditions.checkNotNull(producingTaskExecutorId);
		Preconditions.checkNotNull(resultPartitionDeploymentDescriptor);

		// only blocking partitions require explicit release call
		if (!resultPartitionDeploymentDescriptor.getPartitionType().isBlocking()) {
			return;
		}

		final ResultPartitionID resultPartitionId = resultPartitionDeploymentDescriptor.getShuffleDescriptor().getResultPartitionID();

		partitionInfos.put(resultPartitionId, new PartitionInfo(producingTaskExecutorId, resultPartitionDeploymentDescriptor));
		partitionTable.startTrackingPartitions(producingTaskExecutorId, Collections.singletonList(resultPartitionId));
	}

	@Override
	public void stopTrackingPartitionsFor(ResourceID producingTaskExecutorId) {
		Preconditions.checkNotNull(producingTaskExecutorId);

		// this is a bit icky since we make 2 calls to pT#stopTrackingPartitions
		final Collection<ResultPartitionID> resultPartitionIds = partitionTable.stopTrackingPartitions(producingTaskExecutorId);

		for (ResultPartitionID resultPartitionId : resultPartitionIds) {
			internalStopTrackingPartition(resultPartitionId);
		}
	}

	@Override
	public void stopTrackingAndReleasePartitions(Collection<ResultPartitionID> resultPartitionIds) {
		Preconditions.checkNotNull(resultPartitionIds);

		// stop tracking partitions to be released and group them by task executor ID
		Map<ResourceID, List<ResultPartitionDeploymentDescriptor>> partitionsToReleaseByResourceId = resultPartitionIds.stream()
			.map(this::internalStopTrackingPartition)
			.filter(Optional::isPresent)
			.map(Optional::get)
			.collect(Collectors.groupingBy(
				partitionMetaData -> partitionMetaData.producingTaskExecutorResourceId,
				Collectors.mapping(
					partitionMetaData -> partitionMetaData.resultPartitionDeploymentDescriptor,
					toList())));

		partitionsToReleaseByResourceId.forEach(this::internalReleasePartitions);
	}

	@Override
	public void stopTrackingPartitions(Collection<ResultPartitionID> resultPartitionIds) {
		Preconditions.checkNotNull(resultPartitionIds);

		resultPartitionIds.forEach(this::internalStopTrackingPartition);
	}

	@Override
	public void stopTrackingAndReleasePartitionsFor(ResourceID producingTaskExecutorId) {
		Preconditions.checkNotNull(producingTaskExecutorId);

		// this is a bit icky since we make 2 calls to pT#stopTrackingPartitions
		Collection<ResultPartitionID> resultPartitionIds = partitionTable.stopTrackingPartitions(producingTaskExecutorId);

		stopTrackingAndReleasePartitions(resultPartitionIds);
	}

	@Override
	public boolean isTrackingPartitionsFor(ResourceID producingTaskExecutorId) {
		Preconditions.checkNotNull(producingTaskExecutorId);

		return partitionTable.hasTrackedPartitions(producingTaskExecutorId);
	}

	@Override
	public boolean isPartitionTracked(final ResultPartitionID resultPartitionID) {
		Preconditions.checkNotNull(resultPartitionID);

		return partitionInfos.containsKey(resultPartitionID);
	}

	private Optional<PartitionInfo> internalStopTrackingPartition(ResultPartitionID resultPartitionId) {
		final PartitionInfo partitionInfo = partitionInfos.remove(resultPartitionId);
		if (partitionInfo == null) {
			return Optional.empty();
		}
		partitionTable.stopTrackingPartitions(partitionInfo.producingTaskExecutorResourceId, Collections.singletonList(resultPartitionId));

		return Optional.of(partitionInfo);
	}

	private void internalReleasePartitions(
		ResourceID potentialPartitionLocation,
		Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {

		internalReleasePartitionsOnTaskExecutor(potentialPartitionLocation, partitionDeploymentDescriptors);
		internalReleasePartitionsOnShuffleMaster(partitionDeploymentDescriptors);
	}

	private void internalReleasePartitionsOnTaskExecutor(
		ResourceID potentialPartitionLocation,
		Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {

		final List<ResultPartitionID> partitionsRequiringRpcReleaseCalls = partitionDeploymentDescriptors.stream()
			.map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
			.filter(descriptor -> descriptor.storesLocalResourcesOn().isPresent())
			.map(ShuffleDescriptor::getResultPartitionID)
			.collect(Collectors.toList());

		if (!partitionsRequiringRpcReleaseCalls.isEmpty()) {
			taskExecutorGatewayLookup
				.lookup(potentialPartitionLocation)
				.ifPresent(taskExecutorGateway ->
					taskExecutorGateway.releasePartitions(jobId, partitionsRequiringRpcReleaseCalls));
		}
	}

	private void internalReleasePartitionsOnShuffleMaster(Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {
		partitionDeploymentDescriptors.stream()
			.map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
			.forEach(shuffleMaster::releasePartitionExternally);
	}

	private static final class PartitionInfo {
		public final ResourceID producingTaskExecutorResourceId;
		public final ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor;

		private PartitionInfo(ResourceID producingTaskExecutorResourceId, ResultPartitionDeploymentDescriptor resultPartitionDeploymentDescriptor) {
			this.producingTaskExecutorResourceId = producingTaskExecutorResourceId;
			this.resultPartitionDeploymentDescriptor = resultPartitionDeploymentDescriptor;
		}
	}
}
