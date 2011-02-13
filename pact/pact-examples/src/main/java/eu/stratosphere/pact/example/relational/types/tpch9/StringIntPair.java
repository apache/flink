package eu.stratosphere.pact.example.relational.types.tpch9;

import eu.stratosphere.pact.common.type.base.*;

public class StringIntPair extends PactPair<PactString, PactInteger> {
	public StringIntPair() {
	}

	public StringIntPair(PactString first, PactInteger second) {
		super(first, second);
	}

	public StringIntPair(String first, int second) {
		super(new PactString(first), new PactInteger(second));
	}
}