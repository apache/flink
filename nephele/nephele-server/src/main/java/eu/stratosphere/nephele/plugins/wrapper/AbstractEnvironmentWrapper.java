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

package eu.stratosphere.nephele.plugins.wrapper;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordFactory;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;

/**
 * This class provides an abstract base class for an environment wrapper. An environment wrapper can be used by a plugin
 * to wrap a task's environment and intercept particular method calls. The default implementation of this abstract base
 * class simply forwards every method call to the encapsulated environment.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public abstract class AbstractEnvironmentWrapper implements Environment {

	private final Environment wrappedEnvironment;

	/**
	 * Constructs a new abstract environment wrapper.
	 * 
	 * @param wrappedEnvironment
	 *        the environment to be wrapped
	 */
	public AbstractEnvironmentWrapper(final Environment wrappedEnvironment) {

		if (wrappedEnvironment == null) {
			throw new IllegalArgumentException("Argument wrappedEnvironment must not be null");
		}

		this.wrappedEnvironment = wrappedEnvironment;
	}

	/**
	 * Returns the wrapped environment.
	 * 
	 * @return the wrapped environment
	 */
	protected Environment getWrappedEnvironment() {

		return this.wrappedEnvironment;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.wrappedEnvironment.getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getTaskConfiguration() {

		return this.wrappedEnvironment.getTaskConfiguration();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getJobConfiguration() {

		return this.wrappedEnvironment.getJobConfiguration();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getCurrentNumberOfSubtasks() {

		return this.wrappedEnvironment.getCurrentNumberOfSubtasks();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getIndexInSubtaskGroup() {

		return this.wrappedEnvironment.getIndexInSubtaskGroup();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Thread userThread) {

		this.wrappedEnvironment.userThreadStarted(userThread);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final Thread userThread) {

		this.wrappedEnvironment.userThreadStarted(userThread);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitProvider getInputSplitProvider() {

		return this.wrappedEnvironment.getInputSplitProvider();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOManager getIOManager() {

		return this.wrappedEnvironment.getIOManager();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MemoryManager getMemoryManager() {

		return this.wrappedEnvironment.getMemoryManager();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskName() {
		return this.wrappedEnvironment.getTaskName();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfOutputGates() {

		return this.wrappedEnvironment.getNumberOfOutputGates();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputGates() {

		return this.wrappedEnvironment.getNumberOfInputGates();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> OutputGate<T> createAndRegisterOutputGate(final ChannelSelector<T> selector,
			final boolean isBroadcast) {

		return this.wrappedEnvironment.createAndRegisterOutputGate(selector, isBroadcast);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T extends Record> InputGate<T> createAndRegisterInputGate(final RecordFactory<T> recordFactory) {

		return this.wrappedEnvironment.createAndRegisterInputGate(recordFactory);
	}
}
