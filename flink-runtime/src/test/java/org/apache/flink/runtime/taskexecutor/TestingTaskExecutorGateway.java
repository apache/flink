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
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.Preconditions;

import org.junit.experimental.categories.Category;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * Simple {@link TaskExecutorGateway} implementation for testing purposes.
 */
@Category(New.class)
public class TestingTaskExecutorGateway implements TaskExecutorGateway {

	private final String address;

	private final String hostname;

	private final Consumer<ResourceID> heartbeatJobManagerConsumer;

	private final BiConsumer<JobID, Throwable> disconnectJobManagerConsumer;

	private final BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>> submitTaskConsumer;

	public TestingTaskExecutorGateway(String address, String hostname, Consumer<ResourceID> heartbeatJobManagerConsumer, BiConsumer<JobID, Throwable> disconnectJobManagerConsumer, BiFunction<TaskDeploymentDescriptor, JobMasterId, CompletableFuture<Acknowledge>> submitTaskConsumer) {
		this.address = Preconditions.checkNotNull(address);
		this.hostname = Preconditions.checkNotNull(hostname);
		this.heartbeatJobManagerConsumer = Preconditions.checkNotNull(heartbeatJobManagerConsumer);
		this.disconnectJobManagerConsumer = Preconditions.checkNotNull(disconnectJobManagerConsumer);
		this.submitTaskConsumer = Preconditions.checkNotNull(submitTaskConsumer);
	}

	@Override
	public CompletableFuture<Acknowledge> requestSlot(SlotID slotId, JobID jobId, AllocationID allocationId, String targetAddress, ResourceManagerId resourceManagerId, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
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
	public void failPartition(ExecutionAttemptID executionAttemptID) {
		// noop
	}

	@Override
	public CompletableFuture<Acknowledge> triggerCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointID, long checkpointTimestamp, CheckpointOptions checkpointOptions) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> confirmCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public void heartbeatFromJobManager(ResourceID heartbeatOrigin) {
		heartbeatJobManagerConsumer.accept(heartbeatOrigin);
	}

	@Override
	public void heartbeatFromResourceManager(ResourceID heartbeatOrigin) {
		// noop
	}

	@Override
	public void disconnectJobManager(JobID jobId, Exception cause) {
		disconnectJobManagerConsumer.accept(jobId, cause);
	}

	@Override
	public void disconnectResourceManager(Exception cause) {
		// noop
	}

	@Override
	public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestFileUpload(FileType fileType, Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
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
