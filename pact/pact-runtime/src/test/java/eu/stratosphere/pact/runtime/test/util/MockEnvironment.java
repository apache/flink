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

package eu.stratosphere.pact.runtime.test.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DefaultRecordDeserializer;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RuntimeInputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

public class MockEnvironment implements Environment {

	private final MemoryManager memManager;

	private final IOManager ioManager;

	private final InputSplitProvider inputSplitProvider;

	private final Configuration jobConfiguration;

	private final Configuration taskConfiguration;

	private final List<RuntimeInputGate<PactRecord>> inputs;

	private final List<RuntimeOutputGate<PactRecord>> outputs;

	private final JobID jobID = new JobID();

	public MockEnvironment(long memorySize, MockInputSplitProvider inputSplitProvider) {

		this.jobConfiguration = new Configuration();
		this.taskConfiguration = new Configuration();
		this.inputs = new LinkedList<RuntimeInputGate<PactRecord>>();
		this.outputs = new LinkedList<RuntimeOutputGate<PactRecord>>();

		this.memManager = new DefaultMemoryManager(memorySize);
		this.ioManager = new IOManager(System.getProperty("java.io.tmpdir"));
		this.inputSplitProvider = inputSplitProvider;
	}

	public void addInput(MutableObjectIterator<PactRecord> inputIterator) {
		int id = inputs.size();
		inputs.add(new MockInputGate(id, inputIterator));
	}

	public void addOutput(List<PactRecord> outputList) {
		int id = outputs.size();
		outputs.add(new MockOutputGate(id, outputList));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getTaskConfiguration() {

		return this.taskConfiguration;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public MemoryManager getMemoryManager() {

		return this.memManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public IOManager getIOManager() {

		return this.ioManager;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JobID getJobID() {

		return this.jobID;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputGate<? extends Record> createInputGate(final GateID gateID,
			final RecordDeserializer<? extends Record> deserializer) {

		return this.inputs.remove(0);
	}

	@Override
	public OutputGate<? extends Record> createOutputGate(GateID gateID, Class<? extends Record> outputClass,
			ChannelSelector<? extends Record> selector, boolean isBroadcast) {

		return this.outputs.remove(0);
	}

	private static class MockInputGate extends RuntimeInputGate<PactRecord> {

		private MutableObjectIterator<PactRecord> it;

		public MockInputGate(int id, MutableObjectIterator<PactRecord> it) {
			super(new JobID(), new GateID(), new DefaultRecordDeserializer<PactRecord>(PactRecord.class), id);
			this.it = it;
		}

		@Override
		public PactRecord readRecord(PactRecord target) throws IOException, InterruptedException {

			if (it.next(target)) {
				return target;
			} else {
				return null;
			}
		}
	}

	private static class MockOutputGate extends RuntimeOutputGate<PactRecord> {

		private List<PactRecord> out;

		public MockOutputGate(int index, List<PactRecord> outList) {
			super(new JobID(), new GateID(), PactRecord.class, index, null, false);
			this.out = outList;
		}

		@Override
		public void writeRecord(PactRecord record) throws IOException, InterruptedException {
			out.add(record.createCopy());
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Configuration getJobConfiguration() {

		return this.jobConfiguration;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getCurrentNumberOfSubtasks() {

		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getIndexInSubtaskGroup() {

		return 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadStarted(final Thread userThread) {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void userThreadFinished(final Thread userThread) {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InputSplitProvider getInputSplitProvider() {

		return this.inputSplitProvider;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getTaskName() {

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getNextUnboundInputGateID() {

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public GateID getNextUnboundOutputGateID() {

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfOutputGates() {

		return this.outputs.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getNumberOfInputGates() {

		return this.inputs.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerOutputGate(final OutputGate<? extends Record> outputGate) {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputGate(final InputGate<? extends Record> inputGate) {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDs() {

		throw new IllegalStateException("getOutputChannelIDs called on MockEnvironment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDs() {

		throw new IllegalStateException("getInputChannelIDs called on MockEnvironment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getOutputGateIDs() {

		throw new IllegalStateException("getOutputGateIDs called on MockEnvironment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<GateID> getInputGateIDs() {

		throw new IllegalStateException("getInputGateIDs called on MockEnvironment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getOutputChannelIDsOfGate(final GateID gateID) {

		throw new IllegalStateException("getOutputChannelIDsOfGate called on MockEnvironment");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<ChannelID> getInputChannelIDsOfGate(final GateID gateID) {

		throw new IllegalStateException("getInputChannelIDsOfGate called on MockEnvironment");
	}

}
