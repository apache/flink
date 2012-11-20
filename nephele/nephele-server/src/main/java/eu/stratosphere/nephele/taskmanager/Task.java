/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.taskmanager;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.profiling.TaskManagerProfiler;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.taskmanager.bufferprovider.LocalBufferPoolOwner;
import eu.stratosphere.nephele.taskmanager.routing.RoutingService;
import eu.stratosphere.nephele.taskmanager.routing.TaskContext;

public interface Task {

	/**
	 * Returns the ID of the job this task belongs to.
	 * 
	 * @return the ID of the job this task belongs to
	 */
	JobID getJobID();

	/**
	 * Returns the ID of this task.
	 * 
	 * @return the ID of this task
	 */
	ExecutionVertexID getVertexID();

	/**
	 * Returns the environment associated with this task.
	 * 
	 * @return the environment associated with this task
	 */
	Environment getEnvironment();

	/**
	 * Marks the task as failed and triggers the appropriate state changes.
	 */
	void markAsFailed();

	/**
	 * Checks if the state of the thread which is associated with this task is <code>TERMINATED</code>.
	 * 
	 * @return <code>true</code> if the state of this thread which is associated with this task is
	 *         <code>TERMINATED</code>, <code>false</code> otherwise
	 */
	boolean isTerminated();

	/**
	 * Starts the execution of this task.
	 */
	void startExecution();

	/**
	 * Cancels the execution of the task (i.e. interrupts the execution thread).
	 */
	void cancelExecution();

	/**
	 * Kills the task (i.e. interrupts the execution thread).
	 */
	void killExecution();

	/**
	 * Registers the task manager profiler with the task.
	 * 
	 * @param taskManagerProfiler
	 *        the task manager profiler
	 * @param jobConfiguration
	 *        the configuration attached to the job
	 */
	void registerProfiler(TaskManagerProfiler taskManagerProfiler, Configuration jobConfiguration);

	/**
	 * Unregisters the task from the central memory manager.
	 * 
	 * @param memoryManager
	 *        the central memory manager
	 */
	void unregisterMemoryManager(MemoryManager memoryManager);

	/**
	 * Unregisters the task from the task manager profiler.
	 * 
	 * @param taskManagerProfiler
	 *        the task manager profiler
	 */
	void unregisterProfiler(TaskManagerProfiler taskManagerProfiler);

	/**
	 * Returns the current execution state of the task.
	 * 
	 * @return the current execution state of the task
	 */
	ExecutionState getExecutionState();

	TaskContext createTaskContext(RoutingService routingService, LocalBufferPoolOwner previousBufferPoolOwner);
}
