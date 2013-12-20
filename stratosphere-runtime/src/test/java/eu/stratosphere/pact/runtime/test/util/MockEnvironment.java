/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.test.util;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputChannelResult;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.MutableRecordDeserializerFactory;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordAvailabilityListener;
import eu.stratosphere.nephele.io.RecordDeserializerFactory;
import eu.stratosphere.nephele.io.RuntimeInputGate;
import eu.stratosphere.nephele.io.RuntimeOutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.protocols.AccumulatorProtocol;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.InputSplitProvider;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.MutableObjectIterator;

public class MockEnvironment implements Environment {
	
	private final MemoryManager memManager;

	private final IOManager ioManager;

	private final InputSplitProvider inputSplitProvider;

	private final Configuration jobConfiguration;

	private final Configuration taskConfiguration;

	private final List<RuntimeInputGate<Record>> inputs;

	private final List<RuntimeOutputGate<Record>> outputs;

	private final JobID jobID = new JobID();

	public MockEnvironment(long memorySize, MockInputSplitProvider inputSplitProvider) {
		this.jobConfiguration = new Configuration();
		this.taskConfiguration = new Configuration();
		this.inputs = new LinkedList<RuntimeInputGate<Record>>();
		this.outputs = new LinkedList<RuntimeOutputGate<Record>>();

		this.memManager = new DefaultMemoryManager(memorySize);
		this.ioManager = new IOManager(System.getProperty("java.io.tmpdir"));
		this.inputSplitProvider = inputSplitProvider;
	}

	public void addInput(MutableObjectIterator<Record> inputIterator) {
		int id = inputs.size();
		inputs.add(new MockInputGate(id, inputIterator));
	}

	public void addOutput(List<Record> outputList) {
		int id = outputs.size();
		outputs.add(new MockOutputGate(id, outputList));
	}

	@Override
	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	@Override
	public MemoryManager getMemoryManager() {
		return this.memManager;
	}
	
	@Override
	public IOManager getIOManager() {
		return this.ioManager;
	}

	@Override
	public JobID getJobID() {
		return this.jobID;
	}


	private static class MockInputGate extends RuntimeInputGate<Record> {
		
		private MutableObjectIterator<Record> it;

		public MockInputGate(int id, MutableObjectIterator<Record> it) {
			super(new JobID(), new GateID(), MutableRecordDeserializerFactory.<Record>get(), id);
			this.it = it;
		}

		@Override
		public void registerRecordAvailabilityListener(final RecordAvailabilityListener<Record> listener) {
			super.registerRecordAvailabilityListener(listener);
			this.notifyRecordIsAvailable(0);
		}
		
		@Override
		public InputChannelResult readRecord(Record target) throws IOException, InterruptedException {

			if (it.next(target)) {
				// everything comes from the same source channel and buffer in this mock
				notifyRecordIsAvailable(0);
				return InputChannelResult.INTERMEDIATE_RECORD_FROM_BUFFER;
			} else {
				return InputChannelResult.END_OF_STREAM;
			}
		}
	}

	private static class MockOutputGate extends RuntimeOutputGate<Record> {
		
		private List<Record> out;

		public MockOutputGate(int index, List<Record> outList) {
			super(new JobID(), new GateID(), Record.class, index, null, false);
			this.out = outList;
		}

		@Override
		public void writeRecord(Record record) throws IOException, InterruptedException {
			out.add(record.createCopy());
		}
	}

	@Override
	public Configuration getJobConfiguration() {
		return this.jobConfiguration;
	}

	@Override
	public int getCurrentNumberOfSubtasks() {
		return 1;
	}

	@Override
	public int getIndexInSubtaskGroup() {
		return 0;
	}

	@Override
	public void userThreadStarted(final Thread userThread) {
		// Nothing to do here
	}

	@Override
	public void userThreadFinished(final Thread userThread) {
		// Nothing to do here
	}

	@Override
	public InputSplitProvider getInputSplitProvider() {
		return this.inputSplitProvider;
	}

	@Override
	public String getTaskName() {
		return null;
	}

	@Override
	public GateID getNextUnboundInputGateID() {
		return null;
	}

	@Override
	public GateID getNextUnboundOutputGateID() {
		return null;
	}

	@Override
	public int getNumberOfOutputGates() {
		return this.outputs.size();
	}

	@Override
	public int getNumberOfInputGates() {
		return this.inputs.size();
	}

	@Override
	public void registerOutputGate(final OutputGate<? extends IOReadableWritable> outputGate) {
		// Nothing to do here
	}

	@Override
	public void registerInputGate(final InputGate<? extends IOReadableWritable> inputGate) {
		// Nothing to do here
	}

	@Override
	public Set<ChannelID> getOutputChannelIDs() {
		throw new IllegalStateException("getOutputChannelIDs called on MockEnvironment");
	}

	@Override
	public Set<ChannelID> getInputChannelIDs() {
		throw new IllegalStateException("getInputChannelIDs called on MockEnvironment");
	}

	@Override
	public Set<GateID> getOutputGateIDs() {
		throw new IllegalStateException("getOutputGateIDs called on MockEnvironment");
	}

	@Override
	public Set<GateID> getInputGateIDs() {
		throw new IllegalStateException("getInputGateIDs called on MockEnvironment");
	}

	@Override
	public Set<ChannelID> getOutputChannelIDsOfGate(final GateID gateID) {
		throw new IllegalStateException("getOutputChannelIDsOfGate called on MockEnvironment");
	}

	@Override
	public Set<ChannelID> getInputChannelIDsOfGate(final GateID gateID) {
		throw new IllegalStateException("getInputChannelIDsOfGate called on MockEnvironment");
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends IOReadableWritable> OutputGate<T> createOutputGate(GateID gateID, Class<T> outputClass,
			ChannelSelector<T> selector, boolean isBroadcast)
	{
		return (OutputGate<T>) this.outputs.remove(0);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends IOReadableWritable> InputGate<T> createInputGate(GateID gateID,
			RecordDeserializerFactory<T> deserializerFactory)
	{
		return (InputGate<T>) this.inputs.remove(0);
	}

	@Override
	public int getNumberOfOutputChannels() {
		return this.outputs.size();
	}

	@Override
	public int getNumberOfInputChannels() {
		return this.inputs.size();
	}
	
	@Override
	public AccumulatorProtocol getAccumulatorProtocolProxy() {
		throw new UnsupportedOperationException(
				"getAccumulatorProtocolProxy() is not supported by MockEnvironment");
	}

}
