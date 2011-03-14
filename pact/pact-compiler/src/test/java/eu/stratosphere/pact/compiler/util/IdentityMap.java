package eu.stratosphere.pact.compiler.util;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.type.base.PactInteger;


@SameKey
public final class IdentityMap extends MapStub<PactInteger, PactInteger, PactInteger, PactInteger> {
	@Override
	public void map(PactInteger key, PactInteger value, Collector<PactInteger, PactInteger> out) {
		out.collect(key, value);
	}
}
