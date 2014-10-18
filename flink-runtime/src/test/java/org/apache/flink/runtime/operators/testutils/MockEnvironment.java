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


package org.apache.flink.runtime.operators.testutils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.FutureTask;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.bufferprovider.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.bufferprovider.BufferProvider;
import org.apache.flink.runtime.io.network.bufferprovider.GlobalBufferPool;
import org.apache.flink.runtime.io.network.bufferprovider.LocalBufferPoolOwner;
import org.apache.flink.runtime.io.network.channels.ChannelID;
import org.apache.flink.runtime.io.network.gates.GateID;
import org.apache.flink.runtime.io.network.gates.InputChannelResult;
import org.apache.flink.runtime.io.network.gates.InputGate;
import org.apache.flink.runtime.io.network.gates.OutputGate;
import org.apache.flink.runtime.io.network.gates.RecordAvailabilityListener;
import org.apache.flink.runtime.io.network.serialization.AdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager;
import org.apache.flink.runtime.memorymanager.MemoryManager;
import org.apache.flink.runtime.protocols.AccumulatorProtocol;
import org.apache.flink.types.Record;
import org.apache.flink.util.MutableObjectIterator;

public class MockEnvironment implements Environment, BufferProvider, LocalBufferPoolOwner {
	
	private final MemoryManager memManager;

	private final IOManager ioManager;

	private final InputSplitProvider inputSplitProvider;

	private final Configuration jobConfiguration;

	private final Configuration taskConfiguration;

	private final List<InputGate<Record>> inputs;

	private final List<OutputGate> outputs;

	private final JobID jobID = new JobID();

	private final Buffer mockBuffer;

	public MockEnvironment(long memorySize, MockInputSplitProvider inputSplitProvider, int bufferSize) {
		this.jobConfiguration = new Configuration();
		this.taskConfiguration = new Configuration();
		this.inputs = new LinkedList<InputGate<Record>>();
		this.outputs = new LinkedList<OutputGate>();

		this.memManager = new DefaultMemoryManager(memorySize, 1);
		this.ioManager = new IOManager(System.getProperty("java.io.tmpdir"));
		this.inputSplitProvider = inputSplitProvider;
		this.mockBuffer = new Buffer(new MemorySegment(new byte[bufferSize]), bufferSize, null);
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

	@Override
	public Buffer requestBuffer(int minBufferSize) throws IOException {
		return mockBuffer;
	}

	@Override
	public Buffer requestBufferBlocking(int minBufferSize) throws IOException, InterruptedException {
		return mockBuffer;
	}

	@Override
	public int getBufferSize() {
		return this.mockBuffer.size();
	}

	@Override
	public BufferAvailabilityRegistration registerBufferAvailabilityListener(BufferAvailabilityListener listener) {
		return BufferAvailabilityRegistration.FAILED_BUFFER_POOL_DESTROYED;
	}

	@Override
	public int getNumberOfChannels() {
		return 1;
	}

	@Override
	public void setDesignatedNumberOfBuffers(int numBuffers) {

	}

	@Override
	public void clearLocalBufferPool() {

	}

	@Override
	public void registerGlobalBufferPool(GlobalBufferPool globalBufferPool) {

	}

	@Override
	public void logBufferUtilization() {

	}

	@Override
	public void reportAsynchronousEvent() {

	}

	private static class MockInputGate extends InputGate<Record> {
		
		private MutableObjectIterator<Record> it;

		public MockInputGate(int id, MutableObjectIterator<Record> it) {
			super(new JobID(), new GateID(), id);
			this.it = it;
		}

		@Override
		public void registerRecordAvailabilityListener(final RecordAvailabilityListener<Record> listener) {
			super.registerRecordAvailabilityListener(listener);
			this.notifyRecordIsAvailable(0);
		}
		
		@Override
		public InputChannelResult readRecord(Record target) throws IOException, InterruptedException {

			if ((target = it.next(target)) != null) {
				// everything comes from the same source channel and buffer in this mock
				notifyRecordIsAvailable(0);
				return InputChannelResult.INTERMEDIATE_RECORD_FROM_BUFFER;
			} else {
				return InputChannelResult.END_OF_STREAM;
			}
		}
	}

	private class MockOutputGate extends OutputGate {
		
		private List<Record> out;

		private RecordDeserializer<Record> deserializer;

		private Record record;

		public MockOutputGate(int index, List<Record> outList) {
			super(new JobID(), new GateID(), index);
			this.out = outList;
			this.deserializer = new AdaptiveSpanningRecordDeserializer<Record>();
			this.record = new Record();
		}

		@Override
		public void sendBuffer(Buffer buffer, int targetChannel) throws IOException, InterruptedException {

			this.deserializer.setNextMemorySegment(MockEnvironment.this.mockBuffer.getMemorySegment(), MockEnvironment.this.mockBuffer.size());

			while (this.deserializer.hasUnfinishedData()) {
				DeserializationResult result = this.deserializer.getNextRecord(this.record);

				if (result.isFullRecord()) {
					this.out.add(this.record.createCopy());
				}

				if (result == DeserializationResult.LAST_RECORD_FROM_BUFFER ||
					result == DeserializationResult.PARTIAL_RECORD) {
					break;
				}
			}
		}

		@Override
		public int getNumChannels() {
			return 1;
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
	public int getNumberOfOutputGates() {
		return this.outputs.size();
	}

	@Override
	public int getNumberOfInputGates() {
		return this.inputs.size();
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

	@Override
	public OutputGate createAndRegisterOutputGate() {
		return this.outputs.remove(0);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends IOReadableWritable> InputGate<T> createAndRegisterInputGate() {
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

	@Override
	public ClassLoader getUserClassLoader() {
		return getClass().getClassLoader();
	}

	@Override
	public BufferProvider getOutputBufferProvider() {
		return this;
	}

	@Override
	public Map<String, FutureTask<Path>> getCopyTask() {
		return null;
	}
}
