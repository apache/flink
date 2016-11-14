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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.BatchTask;

/**
 * This is the abstract base class for every task that can be executed by a
 * TaskManager. Concrete tasks like the vertices of batch jobs (see
 * {@link BatchTask} inherit from this class.
 *
 * <p>The TaskManager invokes the {@link #invoke()} method when executing a
 * task. All operations of the task happen in this method (setting up input
 * output stream readers and writers as well as the task's core operation).
 */
public abstract class AbstractInvokable {

	/** The environment assigned to this invokable. */
	private Environment environment;

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
	 * Sets the environment of this task.
	 * 
	 * @param environment
	 *        the environment of this task
	 */
	public final void setEnvironment(Environment environment) {
		this.environment = environment;
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
}
