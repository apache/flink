/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

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
