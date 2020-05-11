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
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.Set;
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

	private Consumer<TaskDeploymentDescriptor> submitConsumer = ignore -> { };

	private Consumer<ExecutionAttemptID> cancelConsumer = ignore -> { };

	private volatile BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction;

	private BiConsumer<JobID, Collection<ResultPartitionID>> releasePartitionsConsumer = (ignore1, ignore2) -> { };

	private CheckpointConsumer checkpointConsumer = (
		executionAttemptID,
		jobId,
		checkpointId,
		timestamp,
		checkpointOptions,
		advanceToEndOfEventTime) -> { };

	public void setSubmitConsumer(Consumer<TaskDeploymentDescriptor> submitConsumer) {
		this.submitConsumer = submitConsumer;
	}

	public void setCancelConsumer(Consumer<ExecutionAttemptID> cancelConsumer) {
		this.cancelConsumer = cancelConsumer;
	}

	public void setFreeSlotFunction(BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> freeSlotFunction) {
		this.freeSlotFunction = freeSlotFunction;
	}

	public void setReleasePartitionsConsumer(BiConsumer<JobID, Collection<ResultPartitionID>> releasePartitionsConsumer) {
		this.releasePartitionsConsumer = releasePartitionsConsumer;
	}

	public void setCheckpointConsumer(CheckpointConsumer checkpointConsumer) {
		this.checkpointConsumer = checkpointConsumer;
	}

	@Override
	public String getAddress() {
		return address;
	}

	@Override
	public CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(
			ExecutionAttemptID executionAttemptID,
			int requestId,
			Time timeout) {
		return FutureUtils.completedExceptionally(new UnsupportedOperationException());
	}

	@Override
	public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
		submitConsumer.accept(tdd);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		cancelConsumer.accept(executionAttemptID);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public CompletableFuture<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	@Override
	public void releasePartitions(JobID jobId, Set<ResultPartitionID> partitionIds) {
		releasePartitionsConsumer.accept(jobId, partitionIds);
	}

	@Override
	public void notifyCheckpointComplete(
			ExecutionAttemptID executionAttemptID,
			JobID jobId,
			long checkpointId,
			long timestamp) {}

	@Override
	public void notifyCheckpointAborted(
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
			CheckpointOptions checkpointOptions,
			boolean advanceToEndOfEventTime) {

		checkpointConsumer.accept(
			executionAttemptID,
			jobId,
			checkpointId,
			timestamp,
			checkpointOptions,
			advanceToEndOfEventTime);
	}

	@Override
	public CompletableFuture<Acknowledge> sendOperatorEventToTask(
			ExecutionAttemptID task,
			OperatorID operator,
			SerializedValue<OperatorEvent> evt) {

		throw new UnsupportedOperationException();
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

	/**
	 * Consumer that accepts checkpoint trigger information.
	 */
	public interface CheckpointConsumer {

		void accept(
			ExecutionAttemptID executionAttemptID,
			JobID jobId,
			long checkpointId,
			long timestamp,
			CheckpointOptions checkpointOptions,
			boolean advanceToEndOfEventTime);
	}
}
