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

package org.apache.flink.runtime.jobmaster.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RescalingBehaviour;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.message.ClassloadingProps;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * {@link JobMasterGateway} implementation for testing purposes.
 */
public class TestingJobMasterGateway implements JobMasterGateway {

	@Override
	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> stop(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> rescaleJob(int newParallelism, RescalingBehaviour rescalingBehaviour, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> rescaleOperators(Collection<JobVertexID> operators, int newParallelism, RescalingBehaviour rescalingBehaviour, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<SerializedInputSplit> requestNextInputSplit(JobVertexID vertexID, ExecutionAttemptID executionAttempt) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<ExecutionState> requestPartitionState(IntermediateDataSetID intermediateResultId, ResultPartitionID partitionId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(ResultPartitionID partitionID, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> disconnectTaskManager(ResourceID resourceID, Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void disconnectResourceManager(ResourceManagerId resourceManagerId, Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<ClassloadingProps> requestClassloadingProps() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Collection<SlotOffer>> offerSlots(ResourceID taskManagerId, Collection<SlotOffer> slots, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void failSlot(ResourceID taskManagerId, AllocationID allocationId, Exception cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<RegistrationResponse> registerTaskManager(String taskManagerRpcAddress, TaskManagerLocation taskManagerLocation, Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void heartbeatFromTaskManager(ResourceID resourceID, AccumulatorReport accumulatorReport) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void heartbeatFromResourceManager(ResourceID resourceID) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<ArchivedExecutionGraph> requestJob(Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable final String targetDirectory, final boolean cancelJob, final Time timeout) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(JobVertexID jobVertexId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void acknowledgeCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void declineCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, Throwable cause) {
		throw new UnsupportedOperationException();
	}

	@Override
	public JobMasterId getFencingToken() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<KvStateLocation> requestKvStateLocation(JobID jobId, String registrationName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateRegistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName, KvStateID kvStateId, InetSocketAddress kvStateServerAddress) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Acknowledge> notifyKvStateUnregistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getAddress() {
		return null;
	}

	@Override
	public String getHostname() {
		return null;
	}
}
