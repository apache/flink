package eu.stratosphere.pact.runtime.histogram;

import java.util.ArrayList;
import java.util.Collections;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class SortStub extends MapStub<PactInteger, PactInteger, PactInteger, PactInteger> {
	ArrayList<PactInteger> values = new ArrayList<PactInteger>();
	Collector<PactInteger, PactInteger> collector;
	
	@Override
	public void open() {
		super.open();
	}

	@Override
	public void map(PactInteger key, PactInteger value, Collector<PactInteger, PactInteger> out) {
		collector = out;
		values.add(value);
	}

	@Override
	public void close() {
		Collections.sort(values);
		
		PactInteger count = new PactInteger(values.size());
		
		for (PactInteger value : values) {
			collector.collect(count, value);
		}
		
		super.close();
	}
}
