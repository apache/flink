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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.runtime.io.network.api.writer.BufferWriter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobID;
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
	 * Returns the index of this subtask in the subtask group.
	 *
	 * @return the index of this subtask in the subtask group
	 */
	int getIndexInSubtaskGroup();

	/**
	 * Returns the input split provider assigned to this environment.
	 *
	 * @return the input split provider or <code>null</code> if no such provider has been assigned to this environment.
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

	String getTaskNameWithSubtasks();

	/**
	 * Returns the proxy object for the accumulator protocol.
	 */
	ActorRef getJobManager();

	/**
	 * Returns the user code class loader
	 */
	ClassLoader getUserClassLoader();

	Map<String, FutureTask<Path>> getCopyTask();

	BroadcastVariableManager getBroadcastVariableManager();

	// ------------------------------------------------------------------------
	// Runtime result writers and readers
	// ------------------------------------------------------------------------
	// The environment sets up buffer-oriented writers and readers, which the
	// user can use to produce and consume results.
	// ------------------------------------------------------------------------

	BufferWriter getWriter(int index);

	BufferWriter[] getAllWriters();

	BufferReader getReader(int index);

	BufferReader[] getAllReaders();
}
