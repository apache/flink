/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.sources.parquet;

import org.apache.flink.table.dataformat.vector.BooleanColumnVector;
import org.apache.flink.table.dataformat.vector.ByteColumnVector;
import org.apache.flink.table.dataformat.vector.BytesColumnVector;
import org.apache.flink.table.dataformat.vector.DoubleColumnVector;
import org.apache.flink.table.dataformat.vector.FloatColumnVector;
import org.apache.flink.table.dataformat.vector.IntegerColumnVector;
import org.apache.flink.table.dataformat.vector.LongColumnVector;

import org.apache.parquet.io.api.Binary;

/**
 * A values reader for Parquet's run-length encoded data for definition/Repetition levels.
 */
public final class VectorizedRleValuesReader extends VectorizedRleValuesReaderBase
		implements VectorizedValuesReader {

	public VectorizedRleValuesReader() {
		super();
	}

	@Override
	public boolean readBoolean() {
		return this.readInteger() != 0;
	}

	@Override
	public void skip() {
		this.readInteger();
	}

	@Override
	public int readValueDictionaryId() {
		return readInteger();
	}

	// The RLE reader implements the vectorized decoding interface when used to decode dictionary
	// IDs. This is different than the above APIs that decodes definitions levels along with values.
	// Since this is only used to decode dictionary IDs, only decoding integers is supported.
	@Override
	public void readIntegers(int total, IntegerColumnVector c, int rowId) {
		int left = total;
		while (left > 0) {
			if (this.currentCount == 0) {
				readNextGroup();
			}
			int n = Math.min(left, this.currentCount);
			switch (mode) {
				case RLE:
					for (int i = 0; i < n; ++i) {
						c.vector[i + rowId] = currentValue;
					}
					break;
				case PACKED:
					System.arraycopy(currentBuffer, currentBufferIdx, c.vector, rowId, n);
					currentBufferIdx += n;
					break;
			}
			rowId += n;
			left -= n;
			currentCount -= n;
		}
	}

	@Override
	public void readDoubles(int total, DoubleColumnVector c, int rowId) {
		throw new UnsupportedOperationException("only readInts is valid.");
	}

	@Override
	public byte readByte() {
		throw new UnsupportedOperationException("only readInts is valid.");
	}

	@Override
	public void readBytes(int total, ByteColumnVector c, int rowId) {
		throw new UnsupportedOperationException("only readInts is valid.");
	}

	@Override
	public void readLongs(int total, LongColumnVector c, int rowId) {
		throw new UnsupportedOperationException("only readInts is valid.");
	}

	@Override
	public void readBinaries(int total, BytesColumnVector c, int rowId) {
		throw new UnsupportedOperationException("only readInts is valid.");
	}

	@Override
	public Binary readBinary(int len) {
		throw new UnsupportedOperationException("only readInts is valid.");
	}

	@Override
	public void readBooleans(int total, BooleanColumnVector c, int rowId) {
		throw new UnsupportedOperationException("only readInts is valid.");
	}

	@Override
	public void readFloats(int total, FloatColumnVector c, int rowId) {
		throw new UnsupportedOperationException("only readInts is valid.");
	}
}

