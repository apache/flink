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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.state.TaskStateHandles;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is the abstract base class for every task that can be executed by a
 * TaskManager. Concrete tasks like the vertices of batch jobs (see
 * {@link BatchTask} inherit from this class.
 *
 * <p>The TaskManager invokes the {@link #invoke()} method when executing a
 * task. All operations of the task happen in this method (setting up input
 * output stream readers and writers as well as the task's core operation).
 *
 * Any subclass that supports recoverable state and participates in
 * checkpointing needs to override {@link #triggerCheckpoint(CheckpointMetaData, CheckpointOptions)},
 * {@link #triggerCheckpointOnBarrier(CheckpointMetaData, CheckpointOptions, CheckpointMetrics)},
 * {@link #abortCheckpointOnBarrier(long, Throwable)} and {@link #notifyCheckpointComplete(long)}.
 */
public abstract class AbstractInvokable {

	/** The environment assigned to this invokable. */
	private Environment environment;

	/** The taskStateHandles assigned to this invokable */
	private TaskStateHandles taskStateHandles;

	/**
	 * Create an Invokable task and set its environment and initial state.
	 * The initial state is typically a snapshot of the state from a previous execution.
	 *
	 * @param environment The environment assigned to this invokable.
	 * @param taskStateHandles The taskStateHandles assigned to this invokable
	 */
	public AbstractInvokable(Environment environment, TaskStateHandles taskStateHandles) {
		this.environment = checkNotNull(environment);
		this.taskStateHandles = taskStateHandles;
	}

	/**
	 * Starts the execution.
	 *
	 * <p>Must be overwritten by the concrete task implementation. This method
	 * is called by the task manager when the actual execution of the task
	 * starts.
	 *
	 * <p>All resources should be cleaned up when the method returns. Make sure
	 * to guard the code with <code>try-finally</code> blocks where necessary.
	 * 
	 * @throws Exception
	 *         Tasks may forward their exceptions for the TaskManager to handle through failure/recovery.
	 */
	public abstract void invoke() throws Exception;

	/**
	 * This method is called when a task is canceled either as a result of a user abort or an execution failure. It can
	 * be overwritten to respond to shut down the user code properly.
	 *
	 * @throws Exception
	 *         thrown if any exception occurs during the execution of the user code
	 */
	public void cancel() throws Exception {
		// The default implementation does nothing.
	}

	/**
	 * Returns the environment of this task.
	 * 
	 * @return The environment of this task.
	 */
	public Environment getEnvironment() {
		return this.environment;
	}

	/**
	 * Returns the user code class loader of this invokable.
	 *
	 * @return user code class loader of this invokable.
	 */
	public ClassLoader getUserCodeClassLoader() {
		return getEnvironment().getUserClassLoader();
	}

	/**
	 * Returns the current number of subtasks the respective task is split into.
	 * 
	 * @return the current number of subtasks the respective task is split into
	 */
	public int getCurrentNumberOfSubtasks() {
		return this.environment.getTaskInfo().getNumberOfParallelSubtasks();
	}

	/**
	 * Returns the index of this subtask in the subtask group.
	 * 
	 * @return the index of this subtask in the subtask group
	 */
	public int getIndexInSubtaskGroup() {
		return this.environment.getTaskInfo().getIndexOfThisSubtask();
	}

	/**
	 * Returns the task configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobVertex}.
	 * 
	 * @return the task configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobVertex}
	 */
	public Configuration getTaskConfiguration() {
		return this.environment.getTaskConfiguration();
	}

	/**
	 * Returns the job configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobGraph}.
	 * 
	 * @return the job configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobGraph}
	 */
	public Configuration getJobConfiguration() {
		return this.environment.getJobConfiguration();
	}

	/**
	 * Returns the global ExecutionConfig.
	 */
	public ExecutionConfig getExecutionConfig() {
		return this.environment.getExecutionConfig();
	}

	/**
	 * Returns the taskStateHandles of this task.
	 *
	 * @return The taskStateHandles of this task.
	 */
	public TaskStateHandles getTaskStateHandles() {
		return this.taskStateHandles;
	}

	/**
	 * Free taskStateHandles for GC
	 */
	protected void clearTaskStateHandles() {
		this.taskStateHandles = null;
	}

	/**
	 * This method is called to trigger a checkpoint, asynchronously by the checkpoint
	 * coordinator.
	 *
	 * <p>This method is called for tasks that start the checkpoints by injecting the initial barriers,
	 * i.e., the source tasks. In contrast, checkpoints on downstream operators, which are the result of
	 * receiving checkpoint barriers, invoke the {@link #triggerCheckpointOnBarrier(CheckpointMetaData, CheckpointOptions, CheckpointMetrics)}
	 * method.
	 *
	 * @param checkpointMetaData Meta data for about this checkpoint
	 * @param checkpointOptions Options for performing this checkpoint
	 *
	 * @return {@code false} if the checkpoint can not be carried out, {@code true} otherwise
	 */
	public abstract boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws Exception;

	/**
	 * This method is called when a checkpoint is triggered as a result of receiving checkpoint
	 * barriers on all input streams.
	 *
	 * @param checkpointMetaData Meta data for about this checkpoint
	 * @param checkpointOptions Options for performing this checkpoint
	 * @param checkpointMetrics Metrics about this checkpoint
	 *
	 * @throws Exception Exceptions thrown as the result of triggering a checkpoint are forwarded.
	 */
	public abstract void triggerCheckpointOnBarrier(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, CheckpointMetrics checkpointMetrics) throws Exception;

	/**
	 * Aborts a checkpoint as the result of receiving possibly some checkpoint barriers,
	 * but at least one {@link org.apache.flink.runtime.io.network.api.CancelCheckpointMarker}.
	 *
	 * <p>This requires implementing tasks to forward a
	 * {@link org.apache.flink.runtime.io.network.api.CancelCheckpointMarker} to their outputs.
	 *
	 * @param checkpointId The ID of the checkpoint to be aborted.
	 * @param cause The reason why the checkpoint was aborted during alignment
	 */
	public abstract void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws Exception;

	/**
	 * Invoked when a checkpoint has been completed, i.e., when the checkpoint coordinator has received
	 * the notification from all participating tasks.
	 *
	 * @param checkpointId The ID of the checkpoint that is complete..
	 * @throws Exception The notification method may forward its exceptions.
	 */
	public abstract void notifyCheckpointComplete(long checkpointId) throws Exception;
}
