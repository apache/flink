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

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.state.StateHandle;

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
	 * Returns the task-wide configuration object, originally attache to the job vertex.
	 *
	 * @return The task-wide configuration
	 */
	Configuration getTaskConfiguration();

	/**
	 * Returns the job-wide configuration object that was attached to the JobGraph.
	 *
	 * @return The job-wide configuration
	 */
	Configuration getJobConfiguration();

	/**
	 * Returns the current number of subtasks the respective task is split into.
	 *
	 * @return the current number of subtasks the respective task is split into
	 */
	int getNumberOfSubtasks();

	/**
	 * Returns the index of this subtask in the subtask group. The index
	 * is between 0 and {@link #getNumberOfSubtasks()} - 1.
	 *
	 * @return the index of this subtask in the subtask group
	 */
	int getIndexInSubtaskGroup();

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
	 * Returns the name of the task running in this environment.
	 *
	 * @return the name of the task running in this environment
	 */
	String getTaskName();

	/**
	 * Returns the name of the task running in this environment, appended
	 * with the subtask indicator, such as "MyTask (3/6)", where
	 * 3 would be ({@link #getIndexInSubtaskGroup()} + 1), and 6 would be
	 * {@link #getNumberOfSubtasks()}.
	 *
	 * @return The name of the task running in this environment, with subtask indicator.
	 */
	String getTaskNameWithSubtasks();

	/**
	 * Returns the user code class loader
	 */
	ClassLoader getUserClassLoader();

	Map<String, Future<Path>> getDistributedCacheEntries();

	BroadcastVariableManager getBroadcastVariableManager();

	/**
	 * Reports the given set of accumulators to the JobManager.
	 *
	 * @param accumulators The accumulators to report.
	 */
	void reportAccumulators(Map<String, Accumulator<?, ?>> accumulators);

	/**
	 * Confirms that the invokable has successfully completed all steps it needed to
	 * to for the checkpoint with the give checkpoint-ID. This method does not include
	 * any state in the checkpoint.
	 * 
	 * @param checkpointId The ID of the checkpoint.
	 */
	void acknowledgeCheckpoint(long checkpointId);

	/**
	 * Confirms that the invokable has successfully completed all steps it needed to
	 * to for the checkpoint with the give checkpoint-ID. This method does include
	 * the given state in the checkpoint.
	 *
	 * @param checkpointId The ID of the checkpoint.
	 * @param state A handle to the state to be included in the checkpoint.   
	 */
	void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state);

	// --------------------------------------------------------------------------------------------
	//  Fields relevant to the I/O system. Should go into Task
	// --------------------------------------------------------------------------------------------

	ResultPartitionWriter getWriter(int index);

	ResultPartitionWriter[] getAllWriters();

	InputGate getInputGate(int index);

	InputGate[] getAllInputGates();
}
