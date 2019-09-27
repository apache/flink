/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.types.SerializableOptional;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Simple {@link TaskExecutorGateway} implementation for testing purposes.
 */
public class TestingTaskExecutorGateway implements TaskExecutorGateway {

	private final String address;

	private final String hostname;

	private final BiConsumer<ResourceID, AllocatedSlotReport> heartbeatJobManagerConsumer;

	private final BiConsumer<JobID, Throwable> disconnectJobManagerConsumer;

	private final BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>> submitTaskConsumer;

	private final Function<Tuple5<SlotID, JobID, AllocationID, String, ResourceManagerId>, CompletableFuture<Acknowledge>> requestSlotFunction;

	private final BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction;

	private final Consumer<ResourceID> heartbeatResourceManagerConsumer;

	private final Consumer<Exception> disconnectResourceManagerConsumer;

	private final Function<ExecutionAttemptID, CompletableFuture<Acknowledge>> cancelTaskFunction;

	private final Supplier<CompletableFuture<Boolean>> canBeReleasedSupplier;

	private final BiConsumer<JobID, Collection<ResultPartitionID>> releasePartitionsConsumer;

	TestingTaskExecutorGateway(
			String address,
			String hostname,
			BiConsumer<ResourceID, AllocatedSlotReport> heartbeatJobManagerConsumer,
			BiConsumer<JobID, Throwable> disconnectJobManagerConsumer,
			BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>> submitTaskConsumer,
			Function<Tuple5<SlotID, JobID, AllocationID, String, ResourceManagerId>, CompletableFuture<Acknowledge>> requestSlotFunction,
			BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction,
			Consumer<ResourceID> heartbeatResourceManagerConsumer,
			Consumer<Exception> disconnectResourceManagerConsumer,
			Function<ExecutionAttemptID, CompletableFuture<Acknowledge>> cancelTaskFunction,
			Supplier<CompletableFuture<Boolean>> canBeReleasedSupplier,
			BiConsumer<JobID, Collection<ResultPartitionID>> releasePartitionsConsumer) {
		this.address = Preconditions.checkNotNull(address);
		this.hostname = Preconditions.checkNotNull(hostname);
		this.heartbeatJobManagerConsumer = Preconditions.checkNotNull(heartbeatJobManagerConsumer);
		this.disconnectJobManagerConsumer = Preconditions.checkNotNull(disconnectJobManagerConsumer);
		this.submitTaskConsumer = Preconditions.checkNotNull(submitTaskConsumer);
		this.requestSlotFunction = Preconditions.checkNotNull(requestSlotFunction);
		this.freeSlotFunction = Preconditions.checkNotNull(freeSlotFunction);
		this.heartbeatResourceManagerConsumer = heartbeatResourceManagerConsumer;
		this.disconnectResourceManagerConsumer = disconnectResourceManagerConsumer;
		this.cancelTaskFunction = cancelTaskFunction;
		this.canBeReleasedSupplier = canBeReleasedSupplier;
		this.releasePartitionsConsumer = releasePartitionsConsumer;
	}

	@Override
	public CompletableFuture<Acknowledge> requestSlot(SlotID slotId, JobID jobId, AllocationID allocationId, String targetAddress, ResourceManagerId resourceManagerId, Time timeout) {
		return requestSlotFunction.apply(Tuple5.of(slotId, jobId, allocationId, targetAddress, resourceManagerId));
	}

	@Override
	public CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
		final ExecutionAttemptID executionAttemptId,
		final int sampleId,
		final int numSamples,
		final Time delayBetweenSamples,
		final int maxStackTraceDepth,
		final Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, JobMasterId jobMasterId, Time timeout) {
		return submitTaskConsumer.apply(tdd, jobMasterId);
	}

	@Override
	public CompletableFuture<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public void releasePartitions(JobID jobId, Collection<ResultPartitionID> partitionIds) {
		releasePartitionsConsumer.accept(jobId, partitionIds);
	}

	@Override
	public CompletableFuture<Acknowledge> triggerCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointID, long checkpointTimestamp, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> confirmCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return cancelTaskFunction.apply(executionAttemptID);
	}

	@Override
	public void heartbeatFromJobManager(ResourceID heartbeatOrigin, AllocatedSlotReport allocatedSlotReport) {
		heartbeatJobManagerConsumer.accept(heartbeatOrigin, allocatedSlotReport);
	}

	@Override
	public void heartbeatFromResourceManager(ResourceID heartbeatOrigin) {
		heartbeatResourceManagerConsumer.accept(heartbeatOrigin);
	}

	@Override
	public void disconnectJobManager(JobID jobId, Exception cause) {
		disconnectJobManagerConsumer.accept(jobId, cause);
	}

	@Override
	public void disconnectResourceManager(Exception cause) {
		disconnectResourceManagerConsumer.accept(cause);
	}

	@Override
	public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
		return freeSlotFunction.apply(allocationId, cause);
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestFileUpload(FileType fileType, Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(Time timeout) {
		return CompletableFuture.completedFuture(SerializableOptional.empty());
	}

	@Override
	public CompletableFuture<Boolean> canBeReleased() {
		return canBeReleasedSupplier.get();
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public String getHostname() {
		return hostname;
	}
}
