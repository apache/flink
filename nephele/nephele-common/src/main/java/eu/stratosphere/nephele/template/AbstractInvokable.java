/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.template;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;

/**
 * Abstract base class for every task class in Nephele.
 * 
 * @author warneke
 */
public abstract class AbstractInvokable {

	/**
	 * The environment assigned to this invokable.
	 */
	private Environment environment = null;

	/**
	 * Must be overwritten by the concrete task to instantiate the required record reader and record writer.
	 */
	public abstract void registerInputOutput();

	/**
	 * Must be overwritten by the concrete task. This method is called by the task manager
	 * when the actual execution of the task starts.
	 * 
	 * @throws Execution
	 *         thrown if any exception occurs during the execution of the tasks
	 */
	public abstract void invoke() throws Exception;

	/**
	 * Sets the environment of this task.
	 * 
	 * @param environment
	 *        the environment of this task
	 */
	public void setEnvironment(Environment environment) {
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
	 * Overwrite this method to implement task specific checks if the
	 * respective task has been configured properly.
	 * 
	 * @throws IllegalConfigurationException
	 *         thrown if the respective tasks is not configured properly
	 */
	public void checkConfiguration() throws IllegalConfigurationException {
		// The default implementation does nothing
	}

	/**
	 * Overwrite this method to provide the minimum number of subtasks the respective task
	 * must be split into at runtime.
	 * 
	 * @return the minimum number of subtasks the respective task must be split into at runtime
	 */
	public int getMinimumNumberOfSubtasks() {
		// The default implementation always returns 1
		return 1;
	}

	/**
	 * Overwrite this method to provide the maximum number of subtasks the respective task
	 * can be split into at runtime.
	 * 
	 * @return the maximum number of subtasks the respective task can be split into at runtime, <code>-1</code> for
	 *         infinity
	 */
	public int getMaximumNumberOfSubtasks() {
		// The default implementation always returns -1
		return -1;
	}

	/**
	 * Returns the current number of subtasks the respective task is split into.
	 * 
	 * @return the current number of subtasks the respective task is split into
	 */
	public int getCurrentNumberOfSubtasks() {

		return this.environment.getCurrentNumberOfSubtasks();
	}

	/**
	 * Returns the index of this subtask in the subtask group.
	 * 
	 * @return the index of this subtask in the subtask group
	 */
	public int getIndexInSubtaskGroup() {

		return this.environment.getIndexInSubtaskGroup();
	}

	/**
	 * Returns the runtime configuration object which was attached to the original {@link JobVertex}.
	 * 
	 * @return the runtime configuration object which was attached to the original {@link JobVertex}
	 */
	public Configuration getRuntimeConfiguration() {

		return this.environment.getRuntimeConfiguration();
	}

	/**
	 * This method should be called by the user code if a custom
	 * user thread has been started.
	 * 
	 * @param userThread
	 *        the user thread which has been started
	 */
	public void userThreadStarted(Thread userThread) {

		if (this.environment != null) {
			this.environment.userThreadStarted(userThread);
		}

	}

	/**
	 * This method should be called by the user code if a custom
	 * user thread has finished.
	 * 
	 * @param userThread
	 *        the user thread which has finished
	 */
	public void userThreadFinished(Thread userThread) {

		if (this.environment != null) {
			this.environment.userThreadFinished(userThread);
		}
	}

}
