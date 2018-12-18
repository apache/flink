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

package org.apache.flink.runtime.executiongraph.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTrace;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * A TaskManagerGateway that simply acks the basic operations (deploy, cancel, update) and does not
 * support any more advanced operations.
 */
public class SimpleAckingTaskManagerGateway implements TaskManagerGateway {

	private final String address = UUID.randomUUID().toString();

	private Optional<Consumer<ExecutionAttemptID>> optSubmitConsumer;

	private Optional<Consumer<ExecutionAttemptID>> optCancelConsumer;

	private volatile BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction;

	@Nonnull
	private volatile BiConsumer<InstanceID, Exception> disconnectFromJobManagerConsumer = (ignoredA, ignoredB) -> {};

	public SimpleAckingTaskManagerGateway() {
		optSubmitConsumer = Optional.empty();
		optCancelConsumer = Optional.empty();
	}

	public void setSubmitConsumer(Consumer<ExecutionAttemptID> predicate) {
		optSubmitConsumer = Optional.of(predicate);
	}

	public void setCancelConsumer(Consumer<ExecutionAttemptID> predicate) {
		optCancelConsumer = Optional.of(predicate);
	}

	public void setFreeSlotFunction(BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction) {
		this.freeSlotFunction = freeSlotFunction;
	}

	public void setDisconnectFromJobManagerConsumer(@Nonnull BiConsumer<InstanceID, Exception> disconnectFromJobManagerConsumer) {
		this.disconnectFromJobManagerConsumer = disconnectFromJobManagerConsumer;
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public void disconnectFromJobManager(InstanceID instanceId, Exception cause) {
		disconnectFromJobManagerConsumer.accept(instanceId, cause);
	}

	@Override
	public void stopCluster(ApplicationStatus applicationStatus, String message) {}

	@Override
	public CompletableFuture<StackTrace> requestStackTrace(Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
			ExecutionAttemptID executionAttemptID,
			int sampleId,
			int numSamples,
			Time delayBetweenSamples,
			int maxStackTraceDepth,
			Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
		optSubmitConsumer.ifPresent(condition -> condition.accept(tdd.getExecutionAttemptId()));
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		optCancelConsumer.ifPresent(condition -> condition.accept(executionAttemptID));
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public void failPartition(ExecutionAttemptID executionAttemptID) {}

	@Override
	public void notifyCheckpointComplete(
			ExecutionAttemptID executionAttemptID,
			JobID jobId,
			long checkpointId,
			long timestamp) {}

	@Override
	public void triggerCheckpoint(
			ExecutionAttemptID executionAttemptID,
			JobID jobId,
			long checkpointId,
			long timestamp,
			CheckpointOptions checkpointOptions) {}

	@Override
	public CompletableFuture<TransientBlobKey> requestTaskManagerLog(Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestTaskManagerStdout(Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
		final BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> currentFreeSlotFunction = freeSlotFunction;

		if (currentFreeSlotFunction != null) {
			return currentFreeSlotFunction.apply(allocationId, cause);
		} else {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}
	}
}
