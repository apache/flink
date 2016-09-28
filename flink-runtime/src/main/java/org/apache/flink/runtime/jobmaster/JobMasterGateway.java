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
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
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
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import java.util.UUID;

/**
 * {@link JobMaster} rpc gateway interface
 */
public interface JobMasterGateway extends CheckpointCoordinatorGateway {

	/**
	 * Starting the job under the given leader session ID.
	 */
	void startJob(final UUID leaderSessionID);

	/**
	 * Suspending job, all the running tasks will be cancelled, and runtime status will be cleared.
	 * Should re-submit the job before restarting it.
	 *
	 * @param cause The reason of why this job been suspended.
	 */
	void suspendJob(final Throwable cause);

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Future flag of the task execution state update result
	 */
	Future<Acknowledge> updateTaskExecutionState(TaskExecutionState taskExecutionState);

	/**
	 * Requesting next input split for the {@link ExecutionJobVertex}. The next input split is sent back to the sender
	 * as a {@link SerializedInputSplit} message.
	 *
	 * @param vertexID         The job vertex id
	 * @param executionAttempt The execution attempt id
	 * @return The future of the input split. If there is no further input split, will return an empty object.
	 */
	Future<SerializedInputSplit> requestNextInputSplit(
		final JobVertexID vertexID,
		final ExecutionAttemptID executionAttempt);

	/**
	 * Requests the current state of the producer of an intermediate result partition.
	 * The state of a partition is currently bound to the state of the producing execution.
	 *
	 * @param jobId                TheID of job that the intermediate result partition belongs to.
	 * @param intermediateResultId The execution attempt ID of the task requesting the partition state.
	 * @param partitionId          The partition ID of the partition to request the state of.
	 * @return The future of the partition state
	 */
	Future<ExecutionState> requestPartitionState(
			JobID jobId,
			IntermediateDataSetID intermediateResultId,
			ResultPartitionID partitionId);

	/**
	 * Notifies the JobManager about available data for a produced partition.
	 * <p>
	 * There is a call to this method for each {@link ExecutionVertex} instance once per produced
	 * {@link ResultPartition} instance, either when first producing data (for pipelined executions)
	 * or when all data has been produced (for staged executions).
	 * <p>
	 * The JobManager then can decide when to schedule the partition consumers of the given session.
	 *
	 * @param partitionID The partition which has already produced data
	 * @param timeout before the rpc call fails
	 * @return Future acknowledge of the schedule or update operation
	 */
	Future<Acknowledge> scheduleOrUpdateConsumers(final ResultPartitionID partitionID, @RpcTimeout final Time timeout);

	/**
	 * Disconnects the given {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} from the
	 * {@link JobMaster}.
	 *
	 * @param resourceID identifying the TaskManager to disconnect
	 */
	void disconnectTaskManager(ResourceID resourceID);
}
