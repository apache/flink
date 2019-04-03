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

package org.apache.flink.table.runtime.util;

import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.MutableObjectIterator;

/**
 * Uniform genarator for binary row.
 */
public class UniformBinaryRowGenerator implements MutableObjectIterator<BinaryRow> {

	int numKeys;
	int numVals;
	int keyCnt = 0;
	int valCnt = 0;
	int startKey = 0;
	int startVal = 0;
	boolean repeatKey;
	private IntValue key = new IntValue();
	private IntValue value = new IntValue();

	public UniformBinaryRowGenerator(int numKeys, int numVals, boolean repeatKey) {
		this(numKeys, numVals, 0, 0, repeatKey);
	}

	public UniformBinaryRowGenerator(
			int numKeys, int numVals, int startKey, int startVal, boolean repeatKey) {
		this.numKeys = numKeys;
		this.numVals = numVals;
		this.startKey = startKey;
		this.startVal = startVal;
		this.repeatKey = repeatKey;
	}

	@Override
	public BinaryRow next(BinaryRow reuse) {
		if (!repeatKey) {
			if (valCnt >= numVals + startVal) {
				return null;
			}

			key.setValue(keyCnt++);
			value.setValue(valCnt);

			if (keyCnt == numKeys + startKey) {
				keyCnt = startKey;
				valCnt++;
			}
		} else {
			if (keyCnt >= numKeys + startKey) {
				return null;
			}
			key.setValue(keyCnt);
			value.setValue(valCnt++);

			if (valCnt == numVals + startVal) {
				valCnt = startVal;
				keyCnt++;
			}
		}

		BinaryRowWriter writer = new BinaryRowWriter(reuse);
		writer.writeInt(0, this.key.getValue());
		writer.writeInt(1, this.value.getValue());
		writer.complete();
		return reuse;
	}

	@Override
	public BinaryRow next() {
		key = new IntValue();
		value = new IntValue();
		return next(new BinaryRow(2));
	}
}
