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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;

public class MockEnvironment extends Environment {

	private MemoryManager memManager;

	private IOManager ioManager;

	private Configuration config;

	private List<InputGate<KeyValuePair<PactInteger, PactInteger>>> inputs;

	private List<OutputGate<KeyValuePair<PactInteger, PactInteger>>> outputs;

	public MockEnvironment(long memorySize) {
		this.config = new Configuration();
		this.inputs = new ArrayList<InputGate<KeyValuePair<PactInteger, PactInteger>>>();
		this.outputs = new ArrayList<OutputGate<KeyValuePair<PactInteger, PactInteger>>>();

		this.memManager = new DefaultMemoryManager(memorySize);
		this.ioManager = new IOManager(System.getProperty("java.io.tmpdir"));
	}

	public void addInput(Iterator<KeyValuePair<PactInteger, PactInteger>> inputIterator) {
		int id = inputs.size();
		inputs.add(new MockInputGate<KeyValuePair<PactInteger, PactInteger>>(id, inputIterator,
			new KeyValuePairDeserializer<PactInteger, PactInteger>(PactInteger.class, PactInteger.class)));
	}

	public void addOutput(List<KeyValuePair<PactInteger, PactInteger>> outputList) {
		int id = outputs.size();
		@SuppressWarnings("unchecked")
		Class<KeyValuePair<PactInteger, PactInteger>> clazz = (Class<KeyValuePair<PactInteger, PactInteger>>) (Class<?>) KeyValuePair.class;
		outputs.add(new MockOutputGate<KeyValuePair<PactInteger, PactInteger>>(id, outputList, clazz));
	}

	@Override
	public Configuration getRuntimeConfiguration() {
		return this.config;
	}

	@Override
	public boolean hasUnboundInputGates() {
		return this.inputs.size() > 0 ? true : false;
	}

	@Override
	public boolean hasUnboundOutputGates() {
		return this.outputs.size() > 0 ? true : false;
	}

	@Override
	public InputGate<? extends Record> getUnboundInputGate(int gateID) {
		return inputs.remove(gateID);
	}

	@Override
	public eu.stratosphere.nephele.io.OutputGate<? extends Record> getUnboundOutputGate(int gateID) {
		return outputs.remove(gateID);
	}

	@Override
	public MemoryManager getMemoryManager() {
		return this.memManager;
	}

	@Override
	public IOManager getIOManager() {
		return this.ioManager;
	}

	private static class MockInputGate<T extends Record> extends InputGate<T> {

		private Iterator<T> it;

		public MockInputGate(int id, Iterator<T> it, RecordDeserializer<T> d) {
			super(new JobID(), new GateID(), d, id, null);
			this.it = it;
		}

		@Override
		public T readRecord() throws IOException, InterruptedException {
			if (it.hasNext()) {
				return it.next();
			} else {
				return null;
			}
		}
	}

	private static class MockOutputGate<T extends Record> extends OutputGate<T> {

		private List<T> out;

		public MockOutputGate(int index, List<T> outList, Class<T> inputClass) {
			super(new JobID(), new GateID(), inputClass, index, null, false);
			this.out = outList;
		}

		@Override
		public void writeRecord(T record) throws IOException, InterruptedException {
			out.add(record);
		}

	}

}
