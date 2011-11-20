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

package eu.stratosphere.nephele.streaming;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RuntimeInputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.util.StringUtils;

public final class TaskWrapper extends AbstractTask implements Environment {

	static final String WRAPPED_CLASS_KEY = "streaming.class.name";

	private AbstractInvokable wrappedInvokable = null;

	private synchronized AbstractInvokable getWrappedInvokable() {

		if (this.wrappedInvokable != null) {
			return this.wrappedInvokable;
		}

		final Configuration conf = getEnvironment().getRuntimeConfiguration();
		final JobID jobID = getEnvironment().getJobID();
		final String className = conf.getString(WRAPPED_CLASS_KEY, null);
		if (className == null) {
			throw new IllegalStateException("Cannot find name of wrapped class");
		}

		try {
			final ClassLoader cl = LibraryCacheManager.getClassLoader(jobID);

			@SuppressWarnings("unchecked")
			final Class<? extends AbstractInvokable> invokableClass = (Class<? extends AbstractInvokable>) Class
				.forName(className, true, cl);

			this.wrappedInvokable = invokableClass.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(StringUtils.stringifyException(e));
		}

		this.wrappedInvokable.setEnvironment(this);

		return this.wrappedInvokable;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {

		getWrappedInvokable().registerInputOutput();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		getWrappedInvokable().invoke();

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return getEnvironment().getJobID();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitProvider getInputSplitProvider() {

		return getEnvironment().getInputSplitProvider();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOManager getIOManager() {

		return getEnvironment().getIOManager();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MemoryManager getMemoryManager() {

		return getEnvironment().getMemoryManager();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskName() {

		return getEnvironment().getTaskName();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfOutputGates() {

		return getEnvironment().getNumberOfOutputGates();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputGates() {

		return getEnvironment().getNumberOfInputGates();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerOutputGate(OutputGate<? extends Record> outputGate) {

		getEnvironment().registerOutputGate(outputGate);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputGate(InputGate<? extends Record> inputGate) {

		getEnvironment().registerInputGate(inputGate);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public OutputGate<? extends Record> createOutputGate(final GateID gateID,
			final Class<? extends Record> outputClass, final ChannelSelector<? extends Record> selector,
			final boolean isBroadcast) {

		return getEnvironment().createOutputGate(gateID, outputClass, selector, isBroadcast);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputGate<? extends Record> createInputGate(final GateID gateID,
			final RecordDeserializer<? extends Record> deserializer,
			final DistributionPattern distributionPattern) {

		return getEnvironment().createInputGate(gateID, deserializer, distributionPattern);
	}

	@Override
	public GateID getNextUnboundInputGateID() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GateID getNextUnboundOutputGateID() {
		// TODO Auto-generated method stub
		return null;
	}
}
