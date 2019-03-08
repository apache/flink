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

package org.apache.flink.table.dataformat;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.util.SegmentsUtil;

/**
 * A utf8 string which is backed by {@link MemorySegment} instead of String. Its data may span
 * multiple {@link MemorySegment}s.
 *
 * <p>Used for internal table-level implementation. The built-in operator will use it for comparison,
 * search, and so on.
 *
 * <p>{@code BinaryString} are influenced by Apache Spark UTF8String.
 */
public class BinaryString extends LazyBinaryFormat<String> {

	private static final long HIGHEST_FIRST_BIT = Long.MIN_VALUE;
	private static final long HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;

	public static final BinaryString EMPTY_UTF8 = BinaryString.fromBytes("".getBytes());

	public BinaryString(MemorySegment[] segments, int offset, int sizeInBytes) {
		super(segments, offset, sizeInBytes);
	}

	public BinaryString(String javaObject) {
		super(javaObject);
	}

	public BinaryString(MemorySegment[] segments, int offset, int sizeInBytes, String javaObject) {
		super(segments, offset, sizeInBytes, javaObject);
	}

	public static BinaryString fromString(String str) {
		if (str == null) {
			return null;
		} else {
			return new BinaryString(str);
		}
	}

	public static BinaryString fromBytes(byte[] bytes) {
		return new BinaryString(
				new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, 0, bytes.length);
	}

	@Override
	public boolean equals(Object o) {
		if (o != null && o instanceof BinaryString) {
			BinaryString other = (BinaryString) o;
			if (javaObject != null && other.javaObject != null) {
				return javaObject.equals(other.javaObject);
			}

			ensureMaterialized();
			other.ensureMaterialized();
			return binaryEquals(other);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		if (javaObject == null) {
			byte[] bytes = SegmentsUtil.allocateReuseBytes(sizeInBytes);
			SegmentsUtil.copyToBytes(segments, offset, bytes, 0, sizeInBytes);
			javaObject = new String(bytes, 0, sizeInBytes);
		}
		return javaObject;
	}

	@Override
	public void materialize() {
		byte[] bytes = javaObject.getBytes();
		segments = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};
		offset = 0;
		sizeInBytes = bytes.length;
	}

	public BinaryString copy() {
		ensureMaterialized();
		byte[] copy = SegmentsUtil.copyToBytes(segments, offset, sizeInBytes);
		return new BinaryString(new MemorySegment[] {MemorySegmentFactory.wrap(copy)},
				offset, sizeInBytes, javaObject);
	}

	/**
	 * Get binary string, if len less than 8, will be include in variablePartOffsetAndLen.
	 *
	 * <p>If len is less than 8, its binary format is:
	 * 1bit mark(1) = 1, 7bits len, and 7bytes data.
	 *
	 * <p>If len is greater or equal to 8, its binary format is:
	 * 1bit mark(1) = 0, 31bits offset, and 4bytes len.
	 * Data is stored in variable-length part.
	 *
	 * <p>Note: Need to consider the ByteOrder.
	 *
	 * @param baseOffset base offset of composite binary format.
	 * @param fieldOffset absolute start offset of 'variablePartOffsetAndLen'.
	 * @param variablePartOffsetAndLen a long value, real data or offset and len.
	 */
	static BinaryString readBinaryStringFieldFromSegments(
			MemorySegment[] segments, int baseOffset, int fieldOffset,
			long variablePartOffsetAndLen) {
		long mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
		if (mark == 0) {
			final int subOffset = (int) (variablePartOffsetAndLen >> 32);
			final int len = (int) variablePartOffsetAndLen;
			return new BinaryString(segments, baseOffset + subOffset, len);
		} else {
			int len = (int) ((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >>> 56);
			if (SegmentsUtil.LITTLE_ENDIAN) {
				return new BinaryString(segments, fieldOffset, len);
			} else {
				// fieldOffset + 1 to skip header.
				return new BinaryString(segments, fieldOffset + 1, len);
			}
		}
	}
}
