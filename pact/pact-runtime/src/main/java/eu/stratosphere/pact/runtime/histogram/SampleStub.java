package eu.stratosphere.pact.runtime.histogram;

import java.util.Random;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class SampleStub extends MapStub<PactInteger, PactInteger, PactInteger, PactInteger> {
	
	private Random rnd;
	private double selectivity;
	
	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		//TODO: Make seed configurable
		//TODO: Make selectivity configurable
	}

	@Override
	public void open() {
		super.open();
		rnd = new Random(1988);
		selectivity = 1;
	}
	
	@Override
	public void map(PactInteger key, PactInteger value, Collector<PactInteger, PactInteger> out) {
		if(rnd.nextInt() % (int)(1/selectivity) == 0) {
			out.collect(key, key);
		}
	}

}
