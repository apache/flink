package eu.stratosphere.pact.runtime.test.util;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class GivenSetInputIterator implements Iterator<KeyValuePair<PactInteger, PactInteger>> {
	
	int[] keys;
	int[] vals;
	
	int itCnt = 0;
	
	public GivenSetInputIterator(int[] keys, int[] vals) {
		this.keys = keys;
		this.vals = vals;
	}
	
	@Override
	public boolean hasNext() {
		return (keys.length > itCnt && vals.length > itCnt);
	}

	@Override
	public KeyValuePair<PactInteger, PactInteger> next() {
		PactInteger key = new PactInteger(keys[itCnt]);
		PactInteger val = new PactInteger(vals[itCnt]);
		itCnt++;
		return new KeyValuePair<PactInteger, PactInteger>(key,val);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}

