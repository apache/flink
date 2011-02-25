package eu.stratosphere.pact.runtime.test.util;

import java.util.Iterator;

import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;

public class RegularlyGeneratedInputGenerator implements Iterator<KeyValuePair<PactInteger, PactInteger>> {

	int numKeys;
	int numVals;
	
	int keyCnt = 0;
	int valCnt = 0;
	
	public RegularlyGeneratedInputGenerator(int numKeys, int numVals) {
		this.numKeys = numKeys;
		this.numVals = numVals;
	}
	
	@Override
	public boolean hasNext() {
		if(valCnt < numVals) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public KeyValuePair<PactInteger, PactInteger> next() {
		PactInteger key = new PactInteger(keyCnt++);
		PactInteger val = new PactInteger(valCnt);
		
		if(keyCnt == numKeys) {
			keyCnt = 0;
			valCnt++;
		}
		
		return new KeyValuePair<PactInteger, PactInteger>(key,val);
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
