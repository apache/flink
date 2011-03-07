package eu.stratosphere.pact.runtime.test.util;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class InfiniteInputIterator implements Iterator<KeyValuePair<PactInteger, PactInteger>> {

	@Override
	public boolean hasNext() {
		return true;
	}

	@Override
	public KeyValuePair<PactInteger, PactInteger> next() {
		return new KeyValuePair<PactInteger, PactInteger>(new PactInteger(0), new PactInteger(0));
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
}
