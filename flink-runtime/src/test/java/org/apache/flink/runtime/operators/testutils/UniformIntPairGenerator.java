/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.runtime.operators.testutils.types.IntPair;
import org.apache.flink.util.MutableObjectIterator;

public class UniformIntPairGenerator implements MutableObjectIterator<IntPair>
{
	final int numKeys;
	final int numVals;
	
	int keyCnt = 0;
	int valCnt = 0;
	boolean repeatKey;
	
	public UniformIntPairGenerator(int numKeys, int numVals, boolean repeatKey) {
		this.numKeys = numKeys;
		this.numVals = numVals;
		this.repeatKey = repeatKey;
	}

	@Override
	public IntPair next(IntPair target) {
		if(!repeatKey) {
			if(valCnt >= numVals) {
				return null;
			}
			
			target.setKey(keyCnt++);
			target.setValue(valCnt);
			
			if(keyCnt == numKeys) {
				keyCnt = 0;
				valCnt++;
			}
		} else {
			if(keyCnt >= numKeys) {
				return null;
			}
			
			target.setKey(keyCnt);
			target.setValue(valCnt++);
			
			if(valCnt == numVals) {
				valCnt = 0;
				keyCnt++;
			}
		}
		
		return target;
	}
}
