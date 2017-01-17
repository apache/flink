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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.message.ClassloadingProps;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.UUID;

/**
 * {@link JobMaster} rpc gateway interface
 */
public interface JobMasterGateway extends CheckpointCoordinatorGateway {

	// ------------------------------------------------------------------------
	//  Job start and stop methods
	// ------------------------------------------------------------------------

	void startJobExecution();

	void suspendExecution(Throwable cause);

	// ------------------------------------------------------------------------

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param leaderSessionID    The leader id of JobManager
	 * @param taskExecutionState New task execution state for a given task
	 * @return Future flag of the task execution state update result
	 */
	Future<Acknowledge> updateTaskExecutionState(
			final UUID leaderSessionID,
			final TaskExecutionState taskExecutionState);

	/**
	 * Requesting next input split for the {@link ExecutionJobVertex}. The next input split is sent back to the sender
	 * as a {@link SerializedInputSplit} message.
	 *
	 * @param leaderSessionID  The leader id of JobManager
	 * @param vertexID         The job vertex id
	 * @param executionAttempt The execution attempt id
	 * @return The future of the input split. If there is no further input split, will return an empty object.
	 */
	Future<SerializedInputSplit> requestNextInputSplit(
			final UUID leaderSessionID,
			final JobVertexID vertexID,
			final ExecutionAttemptID executionAttempt);

	/**
	 * Requests the current state of the partition.
	 * The state of a partition is currently bound to the state of the producing execution.
	 *
	 * @param leaderSessionID The leader id of JobManager
	 * @param intermediateResultId The execution attempt ID of the task requesting the partition state.
	 * @param partitionId          The partition ID of the partition to request the state of.
	 * @return The future of the partition state
	 */
	Future<ExecutionState> requestPartitionState(
			final UUID leaderSessionID,
			final IntermediateDataSetID intermediateResultId,
			final ResultPartitionID partitionId);

	/**
	 * Notifies the JobManager about available data for a produced partition.
	 * <p>
	 * There is a call to this method for each {@link ExecutionVertex} instance once per produced
	 * {@link ResultPartition} instance, either when first producing data (for pipelined executions)
	 * or when all data has been produced (for staged executions).
	 * <p>
	 * The JobManager then can decide when to schedule the partition consumers of the given session.
	 *
	 * @param leaderSessionID The leader id of JobManager
	 * @param partitionID     The partition which has already produced data
	 * @param timeout         before the rpc call fails
	 * @return Future acknowledge of the schedule or update operation
	 */
	Future<Acknowledge> scheduleOrUpdateConsumers(
			final UUID leaderSessionID,
			final ResultPartitionID partitionID,
			@RpcTimeout final Time timeout);

	/**
	 * Disconnects the given {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} from the
	 * {@link JobMaster}.
	 *
	 * @param resourceID identifying the TaskManager to disconnect
	 */
	void disconnectTaskManager(ResourceID resourceID);

	/**
	 * Disconnects the resource manager from the job manager because of the given cause.
	 *
	 * @param jobManagerLeaderId identifying the job manager leader id
	 * @param resourceManagerLeaderId identifying the resource manager leader id
	 * @param cause of the disconnect
	 */
	void disconnectResourceManager(
		final UUID jobManagerLeaderId,
		final UUID resourceManagerLeaderId,
		final Exception cause);

	/**
	 * Requests a {@link KvStateLocation} for the specified {@link KvState} registration name.
	 *
	 * @param registrationName Name under which the KvState has been registered.
	 * @return Future of the requested {@link KvState} location
	 */
	Future<KvStateLocation> lookupKvStateLocation(final String registrationName);

	/**
	 * @param jobVertexId          JobVertexID the KvState instance belongs to.
	 * @param keyGroupRange        Key group range the KvState instance belongs to.
	 * @param registrationName     Name under which the KvState has been registered.
	 * @param kvStateId            ID of the registered KvState instance.
	 * @param kvStateServerAddress Server address where to find the KvState instance.
	 */
	void notifyKvStateRegistered(
			final JobVertexID jobVertexId,
			final KeyGroupRange keyGroupRange,
			final String registrationName,
			final KvStateID kvStateId,
			final KvStateServerAddress kvStateServerAddress);

	/**
	 * @param jobVertexId      JobVertexID the KvState instance belongs to.
	 * @param keyGroupRange    Key group index the KvState instance belongs to.
	 * @param registrationName Name under which the KvState has been registered.
	 */
	void notifyKvStateUnregistered(
			JobVertexID jobVertexId,
			KeyGroupRange keyGroupRange,
			String registrationName);

	/**
	 * Request the classloading props of this job.
	 */
	Future<ClassloadingProps> requestClassloadingProps();

	/**
	 * Offer the given slots to the job manager. The response contains the set of accepted slots.
	 *
	 * @param taskManagerId identifying the task manager
	 * @param slots         to offer to the job manager
	 * @param leaderId      identifying the job leader
	 * @param timeout       for the rpc call
	 * @return Future set of accepted slots.
	 */
	Future<Iterable<SlotOffer>> offerSlots(
			final ResourceID taskManagerId,
			final Iterable<SlotOffer> slots,
			final UUID leaderId,
			@RpcTimeout final Time timeout);

	/**
	 * Fail the slot with the given allocation id and cause.
	 *
	 * @param taskManagerId identifying the task manager
	 * @param allocationId  identifying the slot to fail
	 * @param leaderId      identifying the job leader
	 * @param cause         of the failing
	 */
	void failSlot(final ResourceID taskManagerId,
			final AllocationID allocationId,
			final UUID leaderId,
			final Exception cause);

	/**
	 * Register the task manager at the job manager.
	 *
	 * @param taskManagerRpcAddress the rpc address of the task manager
	 * @param taskManagerLocation   location of the task manager
	 * @param leaderId              identifying the job leader
	 * @param timeout               for the rpc call
	 * @return Future registration response indicating whether the registration was successful or not
	 */
	Future<RegistrationResponse> registerTaskManager(
			final String taskManagerRpcAddress,
			final TaskManagerLocation taskManagerLocation,
			final UUID leaderId,
			@RpcTimeout final Time timeout);
}
