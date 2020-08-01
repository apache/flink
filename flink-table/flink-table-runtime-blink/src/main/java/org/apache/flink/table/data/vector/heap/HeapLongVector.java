/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.vector.heap;

import org.apache.flink.table.data.vector.writable.WritableLongVector;

import java.util.Arrays;

/**
 * This class represents a nullable long column vector.
 */
public class HeapLongVector extends AbstractHeapVector implements WritableLongVector {

	private static final long serialVersionUID = 8534925169458006397L;

	public long[] vector;

	/**
	 * Don't use this except for testing purposes.
	 *
	 * @param len the number of rows
	 */
	public HeapLongVector(int len) {
		super(len);
		vector = new long[len];
	}

	@Override
	public long getLong(int i) {
		if (dictionary == null) {
			return vector[i];
		} else {
			return dictionary.decodeToLong(dictionaryIds.vector[i]);
		}
	}

	@Override
	public void setLong(int i, long value) {
		vector[i] = value;
	}

	@Override
	public void setLongsFromBinary(int rowId, int count, byte[] src, int srcIndex) {
		if (rowId + count > vector.length || srcIndex + count * 8L > src.length) {
			throw new IndexOutOfBoundsException(String.format(
					"Index out of bounds, row id is %s, count is %s, binary src index is %s, binary" +
							" length is %s, long array src index is %s, long array length is %s.",
					rowId, count, srcIndex, src.length, rowId, vector.length));
		}
		if (LITTLE_ENDIAN) {
			UNSAFE.copyMemory(src, BYTE_ARRAY_OFFSET + srcIndex, vector,
					LONG_ARRAY_OFFSET + rowId * 8L, count * 8L);
		} else {
			long srcOffset = srcIndex + BYTE_ARRAY_OFFSET;
			for (int i = 0; i < count; ++i, srcOffset += 8) {
				vector[i + rowId] = Long.reverseBytes(UNSAFE.getLong(src, srcOffset));
			}
		}
	}

	@Override
	public void fill(long value) {
		Arrays.fill(vector, value);
	}
}
