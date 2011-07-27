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

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.runtime.util.ReadingIterator;

public class RegularlyGeneratedInputGenerator implements ReadingIterator<PactRecord> {

	private final PactInteger key = new PactInteger();
	private final PactInteger value = new PactInteger();
	private final PactRecord record = new PactRecord();
	
	int numKeys;
	int numVals;
	
	int keyCnt = 0;
	int valCnt = 0;
	boolean repeatKey;
	
	public RegularlyGeneratedInputGenerator(int numKeys, int numVals, boolean repeatKey) {
		this.numKeys = numKeys;
		this.numVals = numVals;
		this.repeatKey = repeatKey;
	}

	@Override
	public PactRecord next(PactRecord target) {
		if(!repeatKey) {
			if(valCnt >= numVals) {
				return null;
			}
			
			key.setValue(keyCnt++);
			value.setValue(valCnt);
			
			if(keyCnt == numKeys) {
				keyCnt = 0;
				valCnt++;
			}
		} else {
			if(keyCnt >= numKeys) {
				return null;
			}
			key.setValue(keyCnt);
			value.setValue(valCnt++);
			
			if(valCnt == numVals) {
				valCnt = 0;
				keyCnt++;
			}
		}
		
		this.record.setField(0, this.key);
		this.record.setField(1, this.value);
		return this.record;
	}
}
