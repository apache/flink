package eu.stratosphere.pact.compiler.util;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class DummyCrossStub extends CrossStub<PactInteger, PactInteger, PactInteger, PactInteger, PactInteger, PactInteger> {

	@Override
	public void cross(PactInteger key1, PactInteger value1, PactInteger key2, PactInteger value2,
			Collector<PactInteger, PactInteger> out) {
		out.collect(key1, value1);
		out.collect(key2, value2);
	}

}
