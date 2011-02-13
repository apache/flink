package eu.stratosphere.pact.example.relational.types.tpch9;

import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactPair;

public class IntPair extends PactPair<PactInteger, PactInteger> {
	public IntPair() {
	}

	public IntPair(PactInteger first, PactInteger second) {
		super(first, second);
	}

	public IntPair(int first, int second) {
		super(new PactInteger(first), new PactInteger(second));
	}
}