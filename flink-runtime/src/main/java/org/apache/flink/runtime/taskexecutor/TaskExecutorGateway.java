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
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.types.SerializableOptional;

import java.util.concurrent.CompletableFuture;

/**
 * {@link TaskExecutor} RPC gateway interface.
 */
public interface TaskExecutorGateway extends RpcGateway {

	/**
	 * Requests a slot from the TaskManager.
	 *
	 * @param slotId slot id for the request
	 * @param jobId for which to request a slot
	 * @param allocationId id for the request
	 * @param targetAddress to which to offer the requested slots
	 * @param resourceManagerId current leader id of the ResourceManager
	 * @param timeout for the operation
	 * @return answer to the slot request
	 */
	CompletableFuture<Acknowledge> requestSlot(
		SlotID slotId,
		JobID jobId,
		AllocationID allocationId,
		String targetAddress,
		ResourceManagerId resourceManagerId,
		@RpcTimeout Time timeout);

	CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
		ExecutionAttemptID executionAttemptId,
		int sampleId,
		int numSamples,
		Time delayBetweenSamples,
		int maxStackTraceDepth,
		@RpcTimeout Time timeout);

	/**
	 * Submit a {@link Task} to the {@link TaskExecutor}.
	 *
	 * @param tdd describing the task to submit
	 * @param jobMasterId identifying the submitting JobMaster
	 * @param timeout of the submit operation
	 * @return Future acknowledge of the successful operation
	 */
	CompletableFuture<Acknowledge> submitTask(
		TaskDeploymentDescriptor tdd,
		JobMasterId jobMasterId,
		@RpcTimeout Time timeout);

	/**
	 * Update the task where the given partitions can be found.
	 *
	 * @param executionAttemptID identifying the task
	 * @param partitionInfos telling where the partition can be retrieved from
	 * @param timeout for the update partitions operation
	 * @return Future acknowledge if the partitions have been successfully updated
	 */
	CompletableFuture<Acknowledge> updatePartitions(
		ExecutionAttemptID executionAttemptID,
		Iterable<PartitionInfo> partitionInfos,
		@RpcTimeout Time timeout);

	/**
	 * Fail all intermediate result partitions of the given task.
	 *
	 * @param executionAttemptID identifying the task
	 */
	void failPartition(ExecutionAttemptID executionAttemptID);

	/**
	 * Trigger the checkpoint for the given task. The checkpoint is identified by the checkpoint ID
	 * and the checkpoint timestamp.
	 *
	 * @param executionAttemptID identifying the task
	 * @param checkpointID unique id for the checkpoint
	 * @param checkpointTimestamp is the timestamp when the checkpoint has been initiated
	 * @param checkpointOptions for performing the checkpoint
	 * @return Future acknowledge if the checkpoint has been successfully triggered
	 */
	CompletableFuture<Acknowledge> triggerCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointID, long checkpointTimestamp, CheckpointOptions checkpointOptions);

	/**
	 * Confirm a checkpoint for the given task. The checkpoint is identified by the checkpoint ID
	 * and the checkpoint timestamp.
	 *
	 * @param executionAttemptID identifying the task
	 * @param checkpointId unique id for the checkpoint
	 * @param checkpointTimestamp is the timestamp when the checkpoint has been initiated
	 * @return Future acknowledge if the checkpoint has been successfully confirmed
	 */
	CompletableFuture<Acknowledge> confirmCheckpoint(ExecutionAttemptID executionAttemptID, long checkpointId, long checkpointTimestamp);

	/**
	 * Stop the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout for the stop operation
	 * @return Future acknowledge if the task is successfully stopped
	 */
	CompletableFuture<Acknowledge> stopTask(ExecutionAttemptID executionAttemptID, @RpcTimeout Time timeout);

	/**
	 * Cancel the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout for the cancel operation
	 * @return Future acknowledge if the task is successfully canceled
	 */
	CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, @RpcTimeout Time timeout);

	/**
	 * Heartbeat request from the job manager.
	 *
	 * @param heartbeatOrigin unique id of the job manager
	 */
	void heartbeatFromJobManager(ResourceID heartbeatOrigin, AllocatedSlotReport allocatedSlotReport);

	/**
	 * Heartbeat request from the resource manager.
	 *
	 * @param heartbeatOrigin unique id of the resource manager
	 */
	void heartbeatFromResourceManager(ResourceID heartbeatOrigin);

	/**
	 * Disconnects the given JobManager from the TaskManager.
	 *
	 * @param jobId JobID for which the JobManager was the leader
	 * @param cause for the disconnection from the JobManager
	 */
	void disconnectJobManager(JobID jobId, Exception cause);

	/**
	 * Disconnects the ResourceManager from the TaskManager.
	 *
	 * @param cause for the disconnection from the ResourceManager
	 */
	void disconnectResourceManager(Exception cause);

	/**
	 * Frees the slot with the given allocation ID.
	 *
	 * @param allocationId identifying the slot to free
	 * @param cause of the freeing operation
	 * @param timeout for the operation
	 * @return Future acknowledge which is returned once the slot has been freed
	 */
	CompletableFuture<Acknowledge> freeSlot(
		final AllocationID allocationId,
		final Throwable cause,
		@RpcTimeout final Time timeout);

	/**
	 * Requests the file upload of the specified type to the cluster's {@link BlobServer}.
	 *
	 * @param fileType to upload
	 * @param timeout for the asynchronous operation
	 * @return Future which is completed with the {@link TransientBlobKey} of the uploaded file.
	 */
	CompletableFuture<TransientBlobKey> requestFileUpload(FileType fileType, @RpcTimeout Time timeout);

	/**
	 * Returns the fully qualified address of Metric Query Service on the TaskManager.
	 *
	 * @return Future String with Fully qualified (RPC) address of Metric Query Service on the TaskManager.
	 */
	CompletableFuture<SerializableOptional<String>> requestMetricQueryServiceAddress(@RpcTimeout Time timeout);

	/**
	 * Checks whether the task executor can be released. It cannot be released if there're unconsumed result partitions.
	 *
	 * @return Future flag indicating whether the task executor can be released.
	 */
	CompletableFuture<Boolean> canBeReleased();
}
