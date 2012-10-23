/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

