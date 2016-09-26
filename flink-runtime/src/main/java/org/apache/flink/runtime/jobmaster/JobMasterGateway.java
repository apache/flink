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

import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.PartitionState;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.message.NextInputSplit;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import java.util.UUID;

/**
 * {@link JobMaster} rpc gateway interface
 */
public interface JobMasterGateway extends RpcGateway {

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
	Future<Boolean> updateTaskExecutionState(TaskExecutionState taskExecutionState);

	/**
	 * Requesting next input split for the {@link ExecutionJobVertex}. The next input split is sent back to the sender
	 * as a {@link NextInputSplit} message.
	 *
	 * @param vertexID         The job vertex id
	 * @param executionAttempt The execution attempt id
	 * @return The future of the input split. If there is no further input split, will return an empty object.
	 * @throws Exception if some error occurred or information mismatch.
	 */
	Future<NextInputSplit> requestNextInputSplit(
		final JobVertexID vertexID,
		final ExecutionAttemptID executionAttempt) throws Exception;

	/**
	 * Requests the current state of the partition.
	 * The state of a partition is currently bound to the state of the producing execution.
	 *
	 * @param partitionId     The partition ID of the partition to request the state of.
	 * @param taskExecutionId The execution attempt ID of the task requesting the partition state.
	 * @param taskResultId    The input gate ID of the task requesting the partition state.
	 * @return The future of the partition state
	 */
	Future<PartitionState> requestPartitionState(
		final ResultPartitionID partitionId,
		final ExecutionAttemptID taskExecutionId,
		final IntermediateDataSetID taskResultId);

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
	 */
	void scheduleOrUpdateConsumers(final ResultPartitionID partitionID);
}
