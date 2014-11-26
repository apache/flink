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
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for every task class in Flink.
 */
public abstract class AbstractInvokable {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractInvokable.class);


	/**
	 * The environment assigned to this invokable.
	 */
	private volatile Environment environment;

	/**
	 * Must be overwritten by the concrete task to instantiate the required record reader and record writer.
	 */
	public abstract void registerInputOutput();

	/**
	 * Must be overwritten by the concrete task. This method is called by the task manager
	 * when the actual execution of the task starts.
	 * 
	 * @throws Exception
	 *         thrown if any exception occurs during the execution of the tasks
	 */
	public abstract void invoke() throws Exception;

	/**
	 * Sets the environment of this task.
	 * 
	 * @param environment
	 *        the environment of this task
	 */
	public final void setEnvironment(final Environment environment) {
		this.environment = environment;
	}

	/**
	 * Returns the environment of this task.
	 * 
	 * @return the environment of this task or <code>null</code> if the environment has not yet been set
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
	public final int getCurrentNumberOfSubtasks() {
		return this.environment.getNumberOfSubtasks();
	}

	/**
	 * Returns the index of this subtask in the subtask group.
	 * 
	 * @return the index of this subtask in the subtask group
	 */
	public final int getIndexInSubtaskGroup() {
		return this.environment.getIndexInSubtaskGroup();
	}

	/**
	 * Returns the task configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.AbstractJobVertex}.
	 * 
	 * @return the task configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.AbstractJobVertex}
	 */
	public final Configuration getTaskConfiguration() {
		return this.environment.getTaskConfiguration();
	}

	/**
	 * Returns the job configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobGraph}.
	 * 
	 * @return the job configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobGraph}
	 */
	public final Configuration getJobConfiguration() {
		return this.environment.getJobConfiguration();
	}

	/**
	 * Returns the global ExecutionConfig.
	 */
	public ExecutionConfig getExecutionConfig() {
		try {
			ExecutionConfig c = (ExecutionConfig) InstantiationUtil.readObjectFromConfig(
					getJobConfiguration(),
					ExecutionConfig.CONFIG_KEY,
					this.getClass().getClassLoader());
			if (c != null) {
				return c;
			} else {
				return new ExecutionConfig();
			}
		} catch (Exception e) {
			LOG.warn("Could not load ExecutionConfig from Environment, returning default ExecutionConfig: {}", e);
			return new ExecutionConfig();
		}
	}

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
}
