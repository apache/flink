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
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * Utility for tracking partitions and issuing release calls to task executors and shuffle masters.
 */
public class JobMasterPartitionTrackerImpl
	extends AbstractPartitionTracker<ResourceID, ResultPartitionDeploymentDescriptor>
	implements JobMasterPartitionTracker {

	private final JobID jobId;

	private final ShuffleMaster<?> shuffleMaster;

	private final PartitionTrackerFactory.TaskExecutorGatewayLookup taskExecutorGatewayLookup;

	public JobMasterPartitionTrackerImpl(
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

		startTrackingPartition(producingTaskExecutorId, resultPartitionId, resultPartitionDeploymentDescriptor);
	}

	@Override
	public void stopTrackingAndReleasePartitions(Collection<ResultPartitionID> resultPartitionIds) {
		Preconditions.checkNotNull(resultPartitionIds);

		// stop tracking partitions to be released and group them by task executor ID
		Map<ResourceID, List<ResultPartitionDeploymentDescriptor>> partitionsToReleaseByResourceId =
			stopTrackingPartitions(resultPartitionIds)
				.stream()
				.collect(Collectors.groupingBy(
					PartitionTrackerEntry::getKey,
					Collectors.mapping(
						PartitionTrackerEntry::getMetaInfo,
						toList())));

		partitionsToReleaseByResourceId.forEach(this::internalReleasePartitions);
	}

	@Override
	public void stopTrackingAndReleasePartitionsFor(ResourceID producingTaskExecutorId) {
		Preconditions.checkNotNull(producingTaskExecutorId);

		Collection<ResultPartitionDeploymentDescriptor> resultPartitionIds =
			project(stopTrackingPartitionsFor(producingTaskExecutorId), PartitionTrackerEntry::getMetaInfo);

		internalReleasePartitions(producingTaskExecutorId, resultPartitionIds);
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

		final Set<ResultPartitionID> partitionsRequiringRpcReleaseCalls = partitionDeploymentDescriptors.stream()
			.map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
			.filter(descriptor -> descriptor.storesLocalResourcesOn().isPresent())
			.map(ShuffleDescriptor::getResultPartitionID)
			.collect(Collectors.toSet());

		if (!partitionsRequiringRpcReleaseCalls.isEmpty()) {
			taskExecutorGatewayLookup
				.lookup(potentialPartitionLocation)
				.ifPresent(taskExecutorGateway ->
					taskExecutorGateway.releaseOrPromotePartitions(jobId, partitionsRequiringRpcReleaseCalls, Collections.emptySet()));
		}
	}

	private void internalReleasePartitionsOnShuffleMaster(Collection<ResultPartitionDeploymentDescriptor> partitionDeploymentDescriptors) {
		partitionDeploymentDescriptors.stream()
			.map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
			.forEach(shuffleMaster::releasePartitionExternally);
	}

	private static <I, O> Collection<O> project(Collection<I> collection, Function<I, O> projector) {
		return collection
			.stream()
			.map(projector)
			.collect(toList());
	}
}
