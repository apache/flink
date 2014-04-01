package eu.stratosphere.pact.runtime.test.util;

import java.io.IOException;

import eu.stratosphere.pact.runtime.test.util.types.StringPair;
import eu.stratosphere.util.MutableObjectIterator;

public class UniformStringPairGenerator implements MutableObjectIterator<StringPair> {

	final int numKeys;
	final int numVals;
	
	int keyCnt = 0;
	int valCnt = 0;
	boolean repeatKey;
	
	public UniformStringPairGenerator(int numKeys, int numVals, boolean repeatKey) {
		this.numKeys = numKeys;
		this.numVals = numVals;
		this.repeatKey = repeatKey;
	}
	
	@Override
	public StringPair next(StringPair target) throws IOException {
		if(!repeatKey) {
			if(valCnt >= numVals) {
				return null;
			}
			
			target.setKey(Integer.toString(keyCnt++));
			target.setValue(Integer.toBinaryString(valCnt));
			
			if(keyCnt == numKeys) {
				keyCnt = 0;
				valCnt++;
			}
		} else {
			if(keyCnt >= numKeys) {
				return null;
			}
			
			target.setKey(Integer.toString(keyCnt));
			target.setValue(Integer.toBinaryString(valCnt++));
			
			if(valCnt == numVals) {
				valCnt = 0;
				keyCnt++;
			}
		}
		
		return target;
	}

}
