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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTrace;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.taskexecutor.FileType;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;

/**
 * Implementation of the {@link TaskManagerGateway} for Flink's RPC system.
 */
public class RpcTaskManagerGateway implements TaskManagerGateway {

	private final TaskExecutorGateway taskExecutorGateway;

	private final JobMasterId jobMasterId;

	public RpcTaskManagerGateway(TaskExecutorGateway taskExecutorGateway, JobMasterId jobMasterId) {
		this.taskExecutorGateway = Preconditions.checkNotNull(taskExecutorGateway);
		this.jobMasterId = Preconditions.checkNotNull(jobMasterId);
	}

	@Override
	public String getAddress() {
		return taskExecutorGateway.getAddress();
	}

	@Override
	public void disconnectFromJobManager(InstanceID instanceId, Exception cause) {
//		taskExecutorGateway.disconnectFromJobManager(instanceId, cause);
		throw new UnsupportedOperationException("Operation is not yet supported.");
	}

	@Override
	public void stopCluster(ApplicationStatus applicationStatus, String message) {
//		taskExecutorGateway.stopCluster(applicationStatus, message);
		throw new UnsupportedOperationException("Operation is not yet supported.");
	}

	@Override
	public CompletableFuture<StackTrace> requestStackTrace(Time timeout) {
//		return taskExecutorGateway.requestStackTrace(timeout);
		throw new UnsupportedOperationException("Operation is not yet supported.");
	}

	@Override
	public CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
			ExecutionAttemptID executionAttemptID,
			int sampleId,
			int numSamples,
			Time delayBetweenSamples,
			int maxStackTraceDepth,
			Time timeout) {

		return taskExecutorGateway.requestStackTraceSample(
			executionAttemptID,
			sampleId,
			numSamples,
			delayBetweenSamples,
			maxStackTraceDepth,
			timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
		return taskExecutorGateway.submitTask(tdd, jobMasterId, timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return taskExecutorGateway.stopTask(executionAttemptID, timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return taskExecutorGateway.cancelTask(executionAttemptID, timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		return taskExecutorGateway.updatePartitions(executionAttemptID, partitionInfos, timeout);
	}

	@Override
	public void failPartition(ExecutionAttemptID executionAttemptID) {
		taskExecutorGateway.failPartition(executionAttemptID);
	}

	@Override
	public void notifyCheckpointComplete(ExecutionAttemptID executionAttemptID, JobID jobId, long checkpointId, long timestamp) {
		taskExecutorGateway.confirmCheckpoint(executionAttemptID, checkpointId, timestamp);
	}

	@Override
	public void triggerCheckpoint(ExecutionAttemptID executionAttemptID, JobID jobId, long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {
		taskExecutorGateway.triggerCheckpoint(
			executionAttemptID,
			checkpointId,
			timestamp,
			checkpointOptions);
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestTaskManagerLog(Time timeout) {
		return taskExecutorGateway.requestFileUpload(FileType.LOG, timeout);
	}

	@Override
	public CompletableFuture<TransientBlobKey> requestTaskManagerStdout(Time timeout) {
		return taskExecutorGateway.requestFileUpload(FileType.STDOUT, timeout);
	}

	@Override
	public CompletableFuture<Acknowledge> freeSlot(AllocationID allocationId, Throwable cause, Time timeout) {
		return taskExecutorGateway.freeSlot(
			allocationId,
			cause,
			timeout);
	}
}
