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

package org.apache.flink.table.dataformat.vector;

import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.dataformat.vector.heap.HeapBooleanVector;
import org.apache.flink.table.dataformat.vector.heap.HeapByteVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBytesVector;
import org.apache.flink.table.dataformat.vector.heap.HeapDoubleVector;
import org.apache.flink.table.dataformat.vector.heap.HeapFloatVector;
import org.apache.flink.table.dataformat.vector.heap.HeapIntVector;
import org.apache.flink.table.dataformat.vector.heap.HeapLongVector;
import org.apache.flink.table.dataformat.vector.heap.HeapShortVector;

import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link VectorizedColumnBatch}.
 */
public class VectorizedColumnBatchTest {

	private static final int VECTOR_SIZE = 1024;

	@Test
	public void testTyped() {
		HeapBooleanVector col0 = new HeapBooleanVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			col0.vector[i] = i % 2 == 0;
		}

		HeapBytesVector col1 = new HeapBytesVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			byte[] bytes = String.valueOf(i).getBytes(StandardCharsets.UTF_8);
			col1.setVal(i, bytes, 0, bytes.length);
		}

		HeapByteVector col2 = new HeapByteVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			col2.vector[i] = (byte) i;
		}

		HeapDoubleVector col3 = new HeapDoubleVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			col3.vector[i] = i;
		}

		HeapFloatVector col4 = new HeapFloatVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			col4.vector[i] = i;
		}

		HeapIntVector col5 = new HeapIntVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			col5.vector[i] = i;
		}

		HeapLongVector col6 = new HeapLongVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			col6.vector[i] = i;
		}

		HeapShortVector col7 = new HeapShortVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			col7.vector[i] = (short) i;
		}

		VectorizedColumnBatch batch = new VectorizedColumnBatch(
				new ColumnVector[]{col0, col1, col2, col3, col4, col5, col6, col7});

		for (int i = 0; i < VECTOR_SIZE; i++) {
			ColumnarRow row = new ColumnarRow(batch, i);
			assertEquals(row.getBoolean(0), i % 2 == 0);
			assertEquals(row.getString(1).toString(), String.valueOf(i));
			assertEquals(row.getByte(2), (byte) i);
			assertEquals(row.getDouble(3), (double) i, 0);
			assertEquals(row.getFloat(4), (float) i, 0);
			assertEquals(row.getInt(5), i);
			assertEquals(row.getLong(6), (long) i);
			assertEquals(row.getShort(7), (short) i);
		}
	}

	@Test
	public void testNull() {
		// all null
		HeapIntVector col0 = new HeapIntVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			col0.setNullAt(i);
		}

		// some null
		HeapIntVector col1 = new HeapIntVector(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			if (i % 2 == 0) {
				col1.setNullAt(i);
			} else {
				col1.vector[i] = i;
			}
		}

		VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[]{col0, col1});

		for (int i = 0; i < VECTOR_SIZE; i++) {
			ColumnarRow row = new ColumnarRow(batch, i);
			assertTrue(row.isNullAt(0));
			if (i % 2 == 0) {
				assertTrue(row.isNullAt(1));
			} else {
				assertEquals(row.getInt(1), i);
			}
		}
	}

	@Test
	public void testDictionary() {
		// all null
		HeapIntVector col = new HeapIntVector(VECTOR_SIZE);
		int[] dict = new int[2];
		dict[0] = 1998;
		dict[1] = 9998;
		col.setDictionary(new TestDictionary(dict));
		HeapIntVector heapIntVector = col.reserveDictionaryIds(VECTOR_SIZE);
		for (int i = 0; i < VECTOR_SIZE; i++) {
			heapIntVector.vector[i] = i % 2 == 0 ? 0 : 1;
		}

		VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[]{col});

		for (int i = 0; i < VECTOR_SIZE; i++) {
			ColumnarRow row = new ColumnarRow(batch, i);
			if (i % 2 == 0) {
				assertEquals(row.getInt(0), 1998);
			} else {
				assertEquals(row.getInt(0), 9998);
			}
		}
	}

	private final class TestDictionary implements Dictionary {
		private int[] intDictionary;

		public TestDictionary(int[] dictionary) {
			this.intDictionary = dictionary;
		}

		@Override
		public int decodeToInt(int id) {
			return intDictionary[id];
		}

		@Override
		public long decodeToLong(int id) {
			throw new UnsupportedOperationException("Dictionary encoding does not support float");
		}

		@Override
		public float decodeToFloat(int id) {
			throw new UnsupportedOperationException("Dictionary encoding does not support float");
		}

		@Override
		public double decodeToDouble(int id) {
			throw new UnsupportedOperationException("Dictionary encoding does not support double");
		}

		@Override
		public byte[] decodeToBinary(int id) {
			throw new UnsupportedOperationException("Dictionary encoding does not support String");
		}
	}
}
