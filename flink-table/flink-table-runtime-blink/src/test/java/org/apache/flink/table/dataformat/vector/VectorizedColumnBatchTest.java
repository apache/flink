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
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.table.dataformat.vector.heap.HeapBooleanVector;
import org.apache.flink.table.dataformat.vector.heap.HeapByteVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBytesVector;
import org.apache.flink.table.dataformat.vector.heap.HeapDoubleVector;
import org.apache.flink.table.dataformat.vector.heap.HeapFloatVector;
import org.apache.flink.table.dataformat.vector.heap.HeapIntVector;
import org.apache.flink.table.dataformat.vector.heap.HeapLongVector;
import org.apache.flink.table.dataformat.vector.heap.HeapShortVector;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test {@link VectorizedColumnBatch}.
 */
public class VectorizedColumnBatchTest {

	private static final int VECTOR_SIZE = 1024;

	@Test
	public void testTyped() throws IOException {
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

		// The precision of Timestamp in parquet should be one of MILLIS, MICROS or NANOS.
		// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
		//
		// For MILLIS, the underlying INT64 holds milliseconds
		// For MICROS, the underlying INT64 holds microseconds
		// For NANOS, the underlying INT96 holds nanoOfDay(8 bytes) and julianDay(4 bytes)
		long[] vector8 = new long[VECTOR_SIZE];
		for (int i = 0; i < VECTOR_SIZE; i++) {
			vector8[i] = i;
		}
		TimestampColumnVector col8 = new TimestampColumnVector() {
			@Override
			public boolean isNullAt(int i) {
				return false;
			}

			@Override
			public void reset() {

			}

			@Override
			public SqlTimestamp getTimestamp(int i, int precision) {
				return SqlTimestamp.fromEpochMillis(vector8[i]);
			}
		};

		long[] vector9 = new long[VECTOR_SIZE];
		for (int i = 0; i < VECTOR_SIZE; i++) {
			vector9[i] = i * 1000;
		}
		TimestampColumnVector col9 = new TimestampColumnVector() {
			@Override
			public SqlTimestamp getTimestamp(int i, int precision) {
				long microseconds = vector9[i];
				return SqlTimestamp.fromEpochMillis(
						microseconds / 1000, (int) (microseconds % 1000) * 1000);
			}

			@Override
			public boolean isNullAt(int i) {
				return false;
			}

			@Override
			public void reset() {

			}
		};

		HeapBytesVector vector10 = new HeapBytesVector(VECTOR_SIZE);
		{
			int nanosecond = 123456789;
			int start = 0;
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			for (int i = 0; i < VECTOR_SIZE; i++) {
				byte[] bytes = new byte[12];
				long l = i * 1000000000L + nanosecond; // i means second
				for (int j = 0; j < 8; j++) {
					bytes[7 - j] = (byte) l;
					l >>>= 8;
				}
				int n = 2440588; // Epoch Julian
				for (int j = 0; j < 4; j++) {
					bytes[11 - j] = (byte) n;
					n >>>= 8;
				}

				vector10.start[i] = start;
				vector10.length[i] = 12;
				start += 12;
				out.write(bytes);
			}
			vector10.buffer = out.toByteArray();
		}

		TimestampColumnVector col10 = new TimestampColumnVector() {
			@Override
			public SqlTimestamp getTimestamp(int colId, int precision) {
				byte[] bytes = vector10.getBytes(colId).getBytes();
				assert bytes.length == 12;
				long nanoOfDay = 0;
				for (int i = 0; i < 8; i++) {
					nanoOfDay <<= 8;
					nanoOfDay |= (bytes[i] & (0xff));
				}
				int julianDay = 0;
				for (int i = 8; i < 12; i++) {
					julianDay <<= 8;
					julianDay |= (bytes[i] & (0xff));
				}
				long millisecond =
						(julianDay - DateTimeUtils.EPOCH_JULIAN) * DateTimeUtils.MILLIS_PER_DAY + nanoOfDay / 1000000;
				int nanoOfMillisecond = (int) (nanoOfDay % 1000000);
				return SqlTimestamp.fromEpochMillis(millisecond, nanoOfMillisecond);
			}

			@Override
			public boolean isNullAt(int i) {
				return false;
			}

			@Override
			public void reset() {

			}
		};

		long[] vector11 = new long[VECTOR_SIZE];
		DecimalColumnVector col11 = new DecimalColumnVector() {

			@Override
			public boolean isNullAt(int i) {
				return false;
			}

			@Override
			public void reset() {
			}

			@Override
			public Decimal getDecimal(int i, int precision, int scale) {
				return Decimal.fromLong(vector11[i], precision, scale);
			}
		};
		for (int i = 0; i < VECTOR_SIZE; i++) {
			vector11[i] = i;
		}

		VectorizedColumnBatch batch = new VectorizedColumnBatch(new ColumnVector[]{
				col0,
				col1,
				col2,
				col3,
				col4,
				col5,
				col6,
				col7,
				col8,
				col9,
				col10,
				col11});
		batch.setNumRows(VECTOR_SIZE);

		for (int i = 0; i < batch.getNumRows(); i++) {
			ColumnarRow row = new ColumnarRow(batch, i);
			assertEquals(row.getBoolean(0), i % 2 == 0);
			assertEquals(row.getString(1).toString(), String.valueOf(i));
			assertEquals(row.getByte(2), (byte) i);
			assertEquals(row.getDouble(3), i, 0);
			assertEquals(row.getFloat(4), (float) i, 0);
			assertEquals(row.getInt(5), i);
			assertEquals(row.getLong(6), i);
			assertEquals(row.getShort(7), (short) i);
			assertEquals(row.getTimestamp(8, 3).getMillisecond(), i);
			assertEquals(row.getTimestamp(9, 6).getMillisecond(), i);
			assertEquals(row.getTimestamp(10, 9).getMillisecond(), i * 1000L + 123);
			assertEquals(row.getTimestamp(10, 9).getNanoOfMillisecond(), 456789);
			assertEquals(row.getDecimal(11, 10, 0).toUnscaledLong(), i);
		}

		assertEquals(VECTOR_SIZE, batch.getNumRows());
		batch.reset();
		assertEquals(0, batch.getNumRows());
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
