/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for scheduling Flink jobs.
 *
 * <p>Instances are created via {@link SchedulerNGFactory}, and receive a {@link JobGraph} when
 * instantiated.
 *
 * <p>Implementations can expect that methods will not be invoked concurrently. In fact,
 * all invocations will originate from a thread in the {@link ComponentMainThreadExecutor}, which
 * will be passed via {@link #setMainThreadExecutor(ComponentMainThreadExecutor)}.
 */
public interface SchedulerNG {

	void setMainThreadExecutor(ComponentMainThreadExecutor mainThreadExecutor);

	void registerJobStatusListener(JobStatusListener jobStatusListener);

	void startScheduling();

	void suspend(Throwable cause);

	void cancel();

	CompletableFuture<Void> getTerminationFuture();

	void handleGlobalFailure(Throwable cause);

	boolean updateTaskExecutionState(TaskExecutionState taskExecutionState);

	SerializedInputSplit requestNextInputSplit(JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException;

	ExecutionState requestPartitionState(IntermediateDataSetID intermediateResultId, ResultPartitionID resultPartitionId) throws PartitionProducerDisposedException;

	void scheduleOrUpdateConsumers(ResultPartitionID partitionID);

	ArchivedExecutionGraph requestJob();

	JobStatus requestJobStatus();

	JobDetails requestJobDetails();

	// ------------------------------------------------------------------------------------
	// Methods below do not belong to Scheduler but are included due to historical reasons
	// ------------------------------------------------------------------------------------

	KvStateLocation requestKvStateLocation(JobID jobId, String registrationName) throws UnknownKvStateLocation, FlinkJobNotFoundException;

	void notifyKvStateRegistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName, KvStateID kvStateId, InetSocketAddress kvStateServerAddress) throws FlinkJobNotFoundException;

	void notifyKvStateUnregistered(JobID jobId, JobVertexID jobVertexId, KeyGroupRange keyGroupRange, String registrationName) throws FlinkJobNotFoundException;

	// ------------------------------------------------------------------------

	void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot);

	// ------------------------------------------------------------------------

	Optional<OperatorBackPressureStats> requestOperatorBackPressureStats(JobVertexID jobVertexId) throws FlinkException;

	// ------------------------------------------------------------------------

	CompletableFuture<String> triggerSavepoint(@Nullable String targetDirectory, boolean cancelJob);

	void acknowledgeCheckpoint(JobID jobID, ExecutionAttemptID executionAttemptID, long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot checkpointState);

	void declineCheckpoint(DeclineCheckpoint decline);

	CompletableFuture<String> stopWithSavepoint(String targetDirectory, boolean advanceToEndOfEventTime);
}
