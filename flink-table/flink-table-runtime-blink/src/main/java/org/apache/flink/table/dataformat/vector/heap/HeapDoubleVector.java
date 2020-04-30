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

package org.apache.flink.table.dataformat.vector.heap;

import org.apache.flink.table.dataformat.vector.writable.WritableDoubleVector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * This class represents a nullable double precision floating point column vector.
 * This class will be used for operations on all floating point double types
 * and as such will use a 64-bit double value to hold the biggest possible value.
 */
public class HeapDoubleVector extends AbstractHeapVector implements WritableDoubleVector {

	private static final long serialVersionUID = 6193940154117411328L;

	public double[] vector;

	/**
	 * Don't use this except for testing purposes.
	 *
	 * @param len the number of rows
	 */
	public HeapDoubleVector(int len) {
		super(len);
		vector = new double[len];
	}

	@Override
	public double getDouble(int i) {
		if (dictionary == null) {
			return vector[i];
		} else {
			return dictionary.decodeToDouble(dictionaryIds.vector[i]);
		}
	}

	@Override
	public void setDouble(int i, double value) {
		vector[i] = value;
	}

	@Override
	public void setDoublesFromBinary(int rowId, int count, byte[] src, int srcIndex) {
		if (rowId + count > vector.length || srcIndex + count * 8L > src.length) {
			throw new IndexOutOfBoundsException(String.format(
					"Index out of bounds, row id is %s, count is %s, binary src index is %s, binary" +
							" length is %s, double array src index is %s, double array length is %s.",
					rowId, count, srcIndex, src.length, rowId, vector.length));
		}
		if (LITTLE_ENDIAN) {
			UNSAFE.copyMemory(src, BYTE_ARRAY_OFFSET + srcIndex, vector,
					DOUBLE_ARRAY_OFFSET + rowId * 8L, count * 8L);
		} else {
			ByteBuffer bb = ByteBuffer.wrap(src).order(ByteOrder.BIG_ENDIAN);
			for (int i = 0; i < count; ++i) {
				vector[i + rowId] = bb.getDouble(srcIndex + (8 * i));
			}
		}
	}

	@Override
	public void fill(double value) {
		Arrays.fill(vector, value);
	}
}
