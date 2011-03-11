package eu.stratosphere.pact.compiler.util;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public final class DummyInputFormat extends TextInputFormat<PactInteger, PactInteger> {
	PactInteger integer = new PactInteger(1);
	@Override
	public boolean readLine(KeyValuePair<PactInteger, PactInteger> pair, byte[] record) {
		pair.setKey(integer);
		pair.setValue(integer);
		return true;
	}
}