/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.data.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.commons.lang3.RandomStringUtils;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;

/**
 * Utils for testing data formats.
 */
public class DataFormatTestUtil {

	/**
	 * Stringify the given {@link RowData}.
	 */
	public static String rowDataToString(RowData row, LogicalType[] types) {
		checkArgument(types.length == row.getArity());
		StringBuilder build = new StringBuilder();
		build.append(row.getRowKind().shortString()).append("(");
		for (int i = 0; i < row.getArity(); i++) {
			build.append(',');
			if (row.isNullAt(i)) {
				build.append("null");
			} else {
				RowData.FieldGetter fieldGetter = RowData.createFieldGetter(types[i], i);
				build.append(fieldGetter.getFieldOrNull(row));
			}
		}
		build.append(')');
		return build.toString();
	}

	/**
	 * Get a binary row of 24 bytes long.
	 */
	public static BinaryRowData get24BytesBinaryRow() {
		// header (8 bytes) + 2 * string in fixed-length part (8 bytes each)
		BinaryRowData row = new BinaryRowData(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, StringData.fromString(RandomStringUtils.randomNumeric(2)));
		writer.writeString(1, StringData.fromString(RandomStringUtils.randomNumeric(2)));
		writer.complete();
		return row;
	}

	/**
	 * Get a binary row of 160 bytes long.
	 */
	public static BinaryRowData get160BytesBinaryRow() {
		// header (8 bytes) +
		// 72 byte length string (8 bytes in fixed-length, 72 bytes in variable-length) +
		// 64 byte length string (8 bytes in fixed-length, 64 bytes in variable-length)
		BinaryRowData row = new BinaryRowData(2);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.writeString(0, StringData.fromString(RandomStringUtils.randomNumeric(72)));
		writer.writeString(1, StringData.fromString(RandomStringUtils.randomNumeric(64)));
		writer.complete();
		return row;
	}

	/**
	 * Get a binary row consisting of 6 segments.
	 * The bytes of the returned row is the same with the given input binary row.
	 */
	public static BinaryRowData getMultiSeg160BytesBinaryRow(BinaryRowData row160) {
		BinaryRowData multiSegRow160 = new BinaryRowData(2);
		MemorySegment[] segments = new MemorySegment[6];
		int baseOffset = 8;
		int posInSeg = baseOffset;
		int remainSize = 160;
		for (int i = 0; i < segments.length; i++) {
			segments[i] = MemorySegmentFactory.wrap(new byte[32]);
			int copy = Math.min(32 - posInSeg, remainSize);
			row160.getSegments()[0].copyTo(160 - remainSize, segments[i], posInSeg, copy);
			remainSize -= copy;
			posInSeg = 0;
		}
		multiSegRow160.pointTo(segments, baseOffset, 160);
		assertEquals(row160, multiSegRow160);
		return multiSegRow160;
	}

	/**
	 * Get a binary row consisting of 2 segments.
	 * Its first segment is the same with the given input binary row, while its second segment is empty.
	 */
	public static BinaryRowData getMultiSeg160BytesInOneSegRow(BinaryRowData row160) {
		MemorySegment[] segments = new MemorySegment[2];
		segments[0] = row160.getSegments()[0];
		segments[1] = MemorySegmentFactory.wrap(new byte[row160.getSegments()[0].size()]);
		row160.pointTo(segments, 0, row160.getSizeInBytes());
		return row160;
	}

	/**
	 * Split the given byte array into two memory segments.
	 */
	public static MemorySegment[] splitBytes(byte[] bytes, int baseOffset) {
		int newSize = (bytes.length + 1) / 2 + baseOffset;
		MemorySegment[] ret = new MemorySegment[2];
		ret[0] = MemorySegmentFactory.wrap(new byte[newSize]);
		ret[1] = MemorySegmentFactory.wrap(new byte[newSize]);

		ret[0].put(baseOffset, bytes, 0, newSize - baseOffset);
		ret[1].put(0, bytes, newSize - baseOffset, bytes.length - (newSize - baseOffset));
		return ret;
	}

	/**
	 * A simple class for testing generic type getting / setting on data formats.
	 */
	public static class MyObj {
		public int i;
		public double j;

		public MyObj(int i, double j) {
			this.i = i;
			this.j = j;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			MyObj myObj = (MyObj) o;

			return i == myObj.i && Double.compare(myObj.j, j) == 0;
		}

		@Override
		public String toString() {
			return "MyObj{" + "i=" + i + ", j=" + j + '}';
		}
	}
}
