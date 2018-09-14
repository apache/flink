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

package org.apache.flink.runtime.jobmanager.slots;

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
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.StackTrace;
import org.apache.flink.runtime.messages.StackTraceSampleResponse;
import org.apache.flink.runtime.rpc.RpcTimeout;

import java.util.concurrent.CompletableFuture;

/**
 * Task manager gateway interface to communicate with the task manager.
 */
public interface TaskManagerGateway {

	/**
	 * Return the address of the task manager with which the gateway is associated.
	 *
	 * @return Address of the task manager with which this gateway is associated.
	 */
	String getAddress();

	/**
	 * Disconnect the task manager from the job manager.
	 *
	 * @param instanceId identifying the task manager
	 * @param cause of the disconnection
	 */
	void disconnectFromJobManager(InstanceID instanceId, Exception cause);

	/**
	 * Stop the cluster.
	 *
	 * @param applicationStatus to stop the cluster with
	 * @param message to deliver
	 */
	void stopCluster(final ApplicationStatus applicationStatus, final String message);

	/**
	 * Request the stack trace from the task manager.
	 *
	 * @param timeout for the stack trace request
	 * @return Future for a stack trace
	 */
	CompletableFuture<StackTrace> requestStackTrace(final Time timeout);

	/**
	 * Request a stack trace sample from the given task.
	 *
	 * @param executionAttemptID identifying the task to sample
	 * @param sampleId of the sample
	 * @param numSamples to take from the given task
	 * @param delayBetweenSamples to wait for
	 * @param maxStackTraceDepth of the returned sample
	 * @param timeout of the request
	 * @return Future of stack trace sample response
	 */
	CompletableFuture<StackTraceSampleResponse> requestStackTraceSample(
		final ExecutionAttemptID executionAttemptID,
		final int sampleId,
		final int numSamples,
		final Time delayBetweenSamples,
		final int maxStackTraceDepth,
		final Time timeout);

	/**
	 * Submit a task to the task manager.
	 *
	 * @param tdd describing the task to submit
	 * @param timeout of the submit operation
	 * @return Future acknowledge of the successful operation
	 */
	CompletableFuture<Acknowledge> submitTask(
		TaskDeploymentDescriptor tdd,
		Time timeout);

	/**
	 * Stop the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the task is successfully stopped
	 */
	CompletableFuture<Acknowledge> stopTask(
		ExecutionAttemptID executionAttemptID,
		Time timeout);

	/**
	 * Cancel the given task.
	 *
	 * @param executionAttemptID identifying the task
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the task is successfully canceled
	 */
	CompletableFuture<Acknowledge> cancelTask(
		ExecutionAttemptID executionAttemptID,
		Time timeout);

	/**
	 * Update the task where the given partitions can be found.
	 *
	 * @param executionAttemptID identifying the task
	 * @param partitionInfos telling where the partition can be retrieved from
	 * @param timeout of the submit operation
	 * @return Future acknowledge if the partitions have been successfully updated
	 */
	CompletableFuture<Acknowledge> updatePartitions(
		ExecutionAttemptID executionAttemptID,
		Iterable<PartitionInfo> partitionInfos,
		Time timeout);

	/**
	 * Fail all intermediate result partitions of the given task.
	 *
	 * @param executionAttemptID identifying the task
	 */
	void failPartition(ExecutionAttemptID executionAttemptID);

	/**
	 * Notify the given task about a completed checkpoint.
	 *
	 * @param executionAttemptID identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param checkpointId of the completed checkpoint
	 * @param timestamp of the completed checkpoint
	 */
	void notifyCheckpointComplete(
		ExecutionAttemptID executionAttemptID,
		JobID jobId,
		long checkpointId,
		long timestamp);

	/**
	 * Trigger for the given task a checkpoint.
	 *
	 * @param executionAttemptID identifying the task
	 * @param jobId identifying the job to which the task belongs
	 * @param checkpointId of the checkpoint to trigger
	 * @param timestamp of the checkpoint to trigger
	 * @param checkpointOptions of the checkpoint to trigger
	 */
	void triggerCheckpoint(
		ExecutionAttemptID executionAttemptID,
		JobID jobId,
		long checkpointId,
		long timestamp,
		CheckpointOptions checkpointOptions);

	/**
	 * Request the task manager log from the task manager.
	 *
	 * @param timeout for the request
	 * @return Future blob key under which the task manager log has been stored
	 */
	CompletableFuture<TransientBlobKey> requestTaskManagerLog(final Time timeout);

	/**
	 * Request the task manager stdout from the task manager.
	 *
	 * @param timeout for the request
	 * @return Future blob key under which the task manager stdout file has been stored
	 */
	CompletableFuture<TransientBlobKey> requestTaskManagerStdout(final Time timeout);

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
}
