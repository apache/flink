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

package org.apache.flink.runtime.execution;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;

import java.util.Map;
import java.util.concurrent.Future;

/**
 * The Environment gives the code executed in a task access to the task's properties
 * (such as name, parallelism), the configurations, the data stream readers and writers,
 * as well as the various components that are provided by the TaskManager, such as
 * memory manager, I/O manager, ...
 */
public interface Environment {

	/**
	 * Returns the job specific {@link ExecutionConfig}.
	 *
	 * @return The execution configuration associated with the current job.
	 * */
	ExecutionConfig getExecutionConfig();

	/**
	 * Returns the ID of the job that the task belongs to.
	 *
	 * @return the ID of the job from the original job graph
	 */
	JobID getJobID();

	/**
	 * Gets the ID of the JobVertex for which this task executes a parallel subtask.
	 *
	 * @return The JobVertexID of this task.
	 */
	JobVertexID getJobVertexId();

	/**
	 * Gets the ID of the task execution attempt.
	 *
	 * @return The ID of the task execution attempt.
	 */
	ExecutionAttemptID getExecutionId();

	/**
	 * Returns the task-wide configuration object, originally attached to the job vertex.
	 *
	 * @return The task-wide configuration
	 */
	Configuration getTaskConfiguration();

	/**
	 * Gets the task manager info, with configuration and hostname.
	 * 
	 * @return The task manager info, with configuration and hostname. 
	 */
	TaskManagerRuntimeInfo getTaskManagerInfo();

	/**
	 * Returns the task specific metric group.
	 * 
	 * @return The MetricGroup of this task.
     */
	TaskMetricGroup getMetricGroup();

	/**
	 * Returns the job-wide configuration object that was attached to the JobGraph.
	 *
	 * @return The job-wide configuration
	 */
	Configuration getJobConfiguration();

	/**
	 * Returns the {@link TaskInfo} object associated with this subtask
	 *
	 * @return TaskInfo for this subtask
	 */
	TaskInfo getTaskInfo();

	/**
	 * Returns the input split provider assigned to this environment.
	 *
	 * @return The input split provider or {@code null} if no such
	 *         provider has been assigned to this environment.
	 */
	InputSplitProvider getInputSplitProvider();

	/**
	 * Returns the current {@link IOManager}.
	 *
	 * @return the current {@link IOManager}.
	 */
	IOManager getIOManager();

	/**
	 * Returns the current {@link MemoryManager}.
	 *
	 * @return the current {@link MemoryManager}.
	 */
	MemoryManager getMemoryManager();

	/**
	 * Returns the user code class loader
	 */
	ClassLoader getUserClassLoader();

	Map<String, Future<Path>> getDistributedCacheEntries();

	BroadcastVariableManager getBroadcastVariableManager();

	TaskStateManager getTaskStateManager();

	GlobalAggregateManager getGlobalAggregateManager();

	/**
	 * Return the registry for accumulators which are periodically sent to the job manager.
	 * @return the registry
	 */
	AccumulatorRegistry getAccumulatorRegistry();

	/**
	 * Returns the registry for {@link InternalKvState} instances.
	 *
	 * @return KvState registry
	 */
	TaskKvStateRegistry getTaskKvStateRegistry();

	/**
	 * Confirms that the invokable has successfully completed all steps it needed to
	 * to for the checkpoint with the give checkpoint-ID. This method does not include
	 * any state in the checkpoint.
	 * 
	 * @param checkpointId ID of this checkpoint
	 * @param checkpointMetrics metrics for this checkpoint
	 */
	void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics);

	/**
	 * Confirms that the invokable has successfully completed all required steps for
	 * the checkpoint with the give checkpoint-ID. This method does include
	 * the given state in the checkpoint.
	 *
	 * @param checkpointId ID of this checkpoint
	 * @param checkpointMetrics metrics for this checkpoint
	 * @param subtaskState All state handles for the checkpointed state
	 */
	void acknowledgeCheckpoint(long checkpointId, CheckpointMetrics checkpointMetrics, TaskStateSnapshot subtaskState);

	/**
	 * Declines a checkpoint. This tells the checkpoint coordinator that this task will
	 * not be able to successfully complete a certain checkpoint.
	 * 
	 * @param checkpointId The ID of the declined checkpoint.
	 * @param cause An optional reason why the checkpoint was declined.
	 */
	void declineCheckpoint(long checkpointId, Throwable cause);

	/**
	 * Marks task execution failed for an external reason (a reason other than the task code itself
	 * throwing an exception). If the task is already in a terminal state
	 * (such as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
	 * Otherwise it sets the state to FAILED, and, if the invokable code is running,
	 * starts an asynchronous thread that aborts that code.
	 *
	 * <p>This method never blocks.
	 */
	void failExternally(Throwable cause);

	// --------------------------------------------------------------------------------------------
	//  Fields relevant to the I/O system. Should go into Task
	// --------------------------------------------------------------------------------------------

	ResultPartitionWriter getWriter(int index);

	ResultPartitionWriter[] getAllWriters();

	InputGate getInputGate(int index);

	InputGate[] getAllInputGates();

	TaskEventDispatcher getTaskEventDispatcher();
}
