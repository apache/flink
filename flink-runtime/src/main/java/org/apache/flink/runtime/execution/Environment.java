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

import akka.actor.ActorRef;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.MemoryManager;

import java.util.Map;
import java.util.concurrent.FutureTask;

/**
 * The user code of every task runs inside an <code>Environment</code> object.
 * The environment provides important services to the task. It keeps track of
 * setting up the communication channels and provides access to input splits,
 * memory manager, etc.
 */
public interface Environment {

	/**
	 * Returns the ID of the job from the original job graph. It is used by the library cache manager to find the
	 * required
	 * libraries for executing the assigned Nephele task.
	 *
	 * @return the ID of the job from the original job graph
	 */
	JobID getJobID();

	/**
	 * Gets the ID of the jobVertex that this task corresponds to.
	 *
	 * @return The JobVertexID of this task.
	 */
	JobVertexID getJobVertexId();

	/**
	 * Returns the task configuration object which was attached to the original JobVertex.
	 *
	 * @return the task configuration object which was attached to the original JobVertex.
	 */
	Configuration getTaskConfiguration();

	/**
	 * Returns the job configuration object which was attached to the original {@link JobGraph}.
	 *
	 * @return the job configuration object which was attached to the original {@link JobGraph}
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

	Map<String, FutureTask<Path>> getCopyTask();

	BroadcastVariableManager getBroadcastVariableManager();

	/**
	 * Reports the given set of accumulators to the JobManager.
	 *
	 * @param accumulators The accumulators to report.
	 */
	void reportAccumulators(Map<String, Accumulator<?, ?>> accumulators);

	// --------------------------------------------------------------------------------------------
	//  Fields relevant to the I/O system. Should go into Task
	// --------------------------------------------------------------------------------------------

	ResultPartitionWriter getWriter(int index);

	ResultPartitionWriter[] getAllWriters();

	InputGate getInputGate(int index);

	InputGate[] getAllInputGates();


	/**
	 * Returns the proxy object for the accumulator protocol.
	 */
	// THIS DOES NOT BELONG HERE, THIS TOTALLY BREAKS COMPONENTIZATION.
	// THE EXECUTED TASKS HAVE BEEN KEPT INDEPENDENT OF ANY RPC OR ACTOR
	// COMMUNICATION !!!
	ActorRef getJobManager();

}
