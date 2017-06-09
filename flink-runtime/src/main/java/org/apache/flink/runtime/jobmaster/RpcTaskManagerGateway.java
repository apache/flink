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
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTrace;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.Preconditions;

import java.util.UUID;

/**
 * Implementation of the {@link TaskManagerGateway} for Flink's RPC system
 */
public class RpcTaskManagerGateway implements TaskManagerGateway {

	private final TaskExecutorGateway taskExecutorGateway;

	private final UUID leaderId;

	public RpcTaskManagerGateway(TaskExecutorGateway taskExecutorGateway, UUID leaderId) {
		this.taskExecutorGateway = Preconditions.checkNotNull(taskExecutorGateway);
		this.leaderId = Preconditions.checkNotNull(leaderId);
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
	public Future<StackTrace> requestStackTrace(Time timeout) {
//		return taskExecutorGateway.requestStackTrace(timeout);
		throw new UnsupportedOperationException("Operation is not yet supported.");
	}

	@Override
	public Future<StackTraceSampleResponse> requestStackTraceSample(
			ExecutionAttemptID executionAttemptID,
			int sampleId,
			int numSamples,
			Time delayBetweenSamples,
			int maxStackTraceDepth,
			Time timeout) {
//		return taskExecutorGateway.requestStackTraceSample(
//			executionAttemptID,
//			sampleId,
//			numSamples,
//			delayBetweenSamples,
//			maxStackTraceDepth,
//			timeout);

		throw new UnsupportedOperationException("Operation is not yet supported.");
	}

	@Override
	public Future<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
		return taskExecutorGateway.submitTask(tdd, leaderId, timeout);
	}

	@Override
	public Future<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return taskExecutorGateway.stopTask(executionAttemptID, timeout);
	}

	@Override
	public Future<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
		return taskExecutorGateway.cancelTask(executionAttemptID, timeout);
	}

	@Override
	public Future<Acknowledge> updatePartitions(ExecutionAttemptID executionAttemptID, Iterable<PartitionInfo> partitionInfos, Time timeout) {
		return taskExecutorGateway.updatePartitions(executionAttemptID, partitionInfos, timeout);
	}

	@Override
	public void failPartition(ExecutionAttemptID executionAttemptID) {
		taskExecutorGateway.failPartition(executionAttemptID);
	}

	@Override
	public void notifyCheckpointComplete(ExecutionAttemptID executionAttemptID, JobID jobId, long checkpointId, long timestamp) {
//		taskExecutorGateway.notifyCheckpointComplete(executionAttemptID, jobId, checkpointId, timestamp);
		throw new UnsupportedOperationException("Operation is not yet supported.");
	}

	@Override
	public void triggerCheckpoint(ExecutionAttemptID executionAttemptID, JobID jobId, long checkpointId, long timestamp, CheckpointOptions checkpointOptions) {
//		taskExecutorGateway.triggerCheckpoint(executionAttemptID, jobId, checkpointId, timestamp);
		throw new UnsupportedOperationException("Operation is not yet supported.");
	}

	@Override
	public Future<BlobKey> requestTaskManagerLog(Time timeout) {
//		return taskExecutorGateway.requestTaskManagerLog(timeout);
		throw new UnsupportedOperationException("Operation is not yet supported.");
	}

	@Override
	public Future<BlobKey> requestTaskManagerStdout(Time timeout) {
//		return taskExecutorGateway.requestTaskManagerStdout(timeout);
		throw new UnsupportedOperationException("Operation is not yet supported.");
	}
}
