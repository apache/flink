package eu.stratosphere.pact.runtime.task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;

public class MockEnvironment extends Environment {

	private Configuration config;
	
	private List<InputGate<KeyValuePair<PactInteger,PactInteger>>> inputs;
	
	public MockEnvironment() {
		this.config = new Configuration();
		this.inputs = new ArrayList<InputGate<KeyValuePair<PactInteger,PactInteger>>>();
	}
	
	public void addInput(Iterator<KeyValuePair<PactInteger,PactInteger>> inputIterator) {
		int id = inputs.size();
		inputs.add(new MockInputGate<KeyValuePair<PactInteger,PactInteger>>(id, inputIterator, new KeyValuePairDeserializer<PactInteger, PactInteger>(PactInteger.class,PactInteger.class)));
	}
	
	@Override
	public Configuration getRuntimeConfiguration() {
		return this.config;
	}
	
	@Override
	public boolean hasUnboundInputGates() {
		return this.inputs.size()>0?true:false;
	}
	
	@Override
	public InputGate<? extends Record> getUnboundInputGate(int gateID) {
		return inputs.remove(gateID);
	}
	
	private static class MockInputGate<T extends Record> extends InputGate<T> {

		private Iterator<T> it;
		
		public MockInputGate(int id, Iterator<T> it, RecordDeserializer<T> d) {
			super(d,id,null);
			this.it = it;
		}
		
		@Override
		public T readRecord() throws IOException, InterruptedException {
			if(it.hasNext()) {
				return it.next();
			} else {
				return null;
			}
		}
	}
	
	
}
