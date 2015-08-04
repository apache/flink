/*
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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.MutableObjectIterator;

public class UniformIntStringTupleGenerator implements MutableObjectIterator<Tuple2<Integer, String>> {

	private final int numKeys;
	private final int numVals;
	
	private int keyCnt;
	private int valCnt;
	
	private boolean repeatKey;
	
	
	public UniformIntStringTupleGenerator(int numKeys, int numVals, boolean repeatKey) {
		this.numKeys = numKeys;
		this.numVals = numVals;
		this.repeatKey = repeatKey;
	}
	
	@Override
	public Tuple2<Integer, String> next(Tuple2<Integer, String> target) {
		if (!repeatKey) {
			if(valCnt >= numVals) {
				return null;
			}
			
			target.f0 = keyCnt++;
			target.f1 = Integer.toBinaryString(valCnt);
			
			if(keyCnt == numKeys) {
				keyCnt = 0;
				valCnt++;
			}
		}
		else {
			if (keyCnt >= numKeys) {
				return null;
			}
			
			target.f0 = keyCnt;
			target.f1 = Integer.toBinaryString(valCnt++);
			
			if (valCnt == numVals) {
				valCnt = 0;
				keyCnt++;
			}
		}
		
		return target;
	}

	@Override
	public Tuple2<Integer, String> next() {
		return next(new Tuple2<Integer, String>());
	}
}
