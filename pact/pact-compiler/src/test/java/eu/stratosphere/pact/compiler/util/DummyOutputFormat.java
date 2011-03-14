package eu.stratosphere.pact.compiler.util;


import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;


public final class DummyOutputFormat extends TextOutputFormat<PactInteger, PactInteger> {
	private final byte[] bytes = new byte[0];
	@Override
	public byte[] writeLine(KeyValuePair<PactInteger, PactInteger> pair) {
		return bytes;
	}
}