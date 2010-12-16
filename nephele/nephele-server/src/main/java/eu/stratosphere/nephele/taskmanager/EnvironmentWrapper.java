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

package eu.stratosphere.nephele.taskmanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.ExecutionNotifiable;
import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;

/**
 * The {@link EnvironmentWrapper} class is an auxiliary class. Its only purpose
 * is to attach an {@link ExecutionVertexID} to an {@link Environment} object and
 * forward notifications about state changes to the task manager.
 * 
 * @author warneke
 */
public class EnvironmentWrapper implements ExecutionNotifiable {

	private static final Log LOG = LogFactory.getLog(EnvironmentWrapper.class);

	/**
	 * The task manager to forward state changes to.
	 */
	private final TaskManager taskManager;

	/**
	 * The id of the corresponding execution vertex.
	 */
	private final ExecutionVertexID id;

	/**
	 * The environment this wrapper belongs to.
	 */
	private final Environment environment;

	/**
	 * Constructs a new {@link EnvironmentWrapper} object.
	 * 
	 * @param taskManager
	 *        the task manager to forward state changes to
	 * @param id
	 *        the id of the corresponding execution vertex
	 * @param environment
	 *        the environment this wrapper belongs to
	 */
	public EnvironmentWrapper(TaskManager taskManager, ExecutionVertexID id, Environment environment) {

		this.taskManager = taskManager;
		this.id = id;
		this.environment = environment;
	}

	/**
	 * Returns the {@link Environment} object attached to this wrapper.
	 * 
	 * @return the {@link Environment} object attached to this wrapper
	 */
	public Environment getEnvironment() {
		return this.environment;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void executionStateChanged(Environment ee, ExecutionState newExecutionState, String optionalMessage) {

		if (newExecutionState == ExecutionState.FAILED) {
			LOG.error(optionalMessage);
		}

		this.taskManager.executionStateChanged(this.environment.getJobID(), this.id, this.environment,
			newExecutionState, optionalMessage);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(Environment ee, Thread userThread) {
		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(Environment ee, Thread userThread) {
		// Nothing to do here
	}

}
