package eu.stratosphere.pact.compiler.util;

import java.util.Iterator;

import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;


@SameKey
public final class IdentityReduce extends ReduceStub<PactInteger, PactInteger, PactInteger, PactInteger> {
	@Override
	public void reduce(PactInteger key, Iterator<PactInteger> values, Collector<PactInteger, PactInteger> out) {
		while (values.hasNext()) {
			out.collect(key, values.next());
		}
	}
}