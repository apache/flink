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

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A utf8 string which is backed by {@link MemorySegment} instead of String. Its data may span
 * multiple {@link MemorySegment}s.
 *
 * <p>Used for internal table-level implementation. The built-in operator will use it for comparison,
 * search, and so on.
 *
 * <p>{@code BinaryString} are influenced by Apache Spark UTF8String.
 */
public class BinaryString extends LazyBinaryFormat<String> implements Comparable<BinaryString> {

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

	/**
	 * Creates an BinaryString from given address (base and offset) and length.
	 */
	public static BinaryString fromAddress(
			MemorySegment[] segments, int offset, int numBytes) {
		return new BinaryString(segments, offset, numBytes);
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

	/**
	 * Creates an BinaryString that contains `length` spaces.
	 */
	public static BinaryString blankString(int length) {
		byte[] spaces = new byte[length];
		Arrays.fill(spaces, (byte) ' ');
		return fromBytes(spaces);
	}

	public byte getByte(int i) {
		ensureMaterialized();
		int globalOffset = offset + i;
		int size = segments[0].size();
		if (globalOffset < size) {
			return segments[0].get(globalOffset);
		} else {
			return segments[globalOffset / size].get(globalOffset % size);
		}
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
	 * UTF-8 supports bytes comparison.
	 */
	@Override
	public int compareTo(BinaryString other) {

		if (javaObject != null && other.javaObject != null) {
			return javaObject.compareTo(other.javaObject);
		}

		ensureMaterialized();
		other.ensureMaterialized();
		if (segments.length == 1 && other.segments.length == 1) {

			int len = Math.min(sizeInBytes, other.sizeInBytes);
			MemorySegment seg1 = segments[0];
			MemorySegment seg2 = other.segments[0];

			for (int i = 0; i < len; i++) {
				int res = (seg1.get(offset + i) & 0xFF) - (seg2.get(other.offset + i) & 0xFF);
				if (res != 0) {
					return res;
				}
			}
			return sizeInBytes - other.sizeInBytes;
		}

		// if there are multi segments.
		return compareMultiSegments(other);
	}

	/**
	 * Find the boundaries of segments, and then compare MemorySegment.
	 */
	private int compareMultiSegments(BinaryString other) {

		if (sizeInBytes == 0 || other.sizeInBytes == 0) {
			return sizeInBytes - other.sizeInBytes;
		}

		int len = Math.min(sizeInBytes, other.sizeInBytes);

		MemorySegment seg1 = segments[0];
		MemorySegment seg2 = other.segments[0];

		int segmentSize = segments[0].size();
		int otherSegmentSize = other.segments[0].size();

		int sizeOfFirst1 = segmentSize - offset;
		int sizeOfFirst2 = otherSegmentSize - other.offset;

		int varSegIndex1 = 1;
		int varSegIndex2 = 1;

		// find the first segment of this string.
		while (sizeOfFirst1 <= 0) {
			sizeOfFirst1 += segmentSize;
			seg1 = segments[varSegIndex1++];
		}

		while (sizeOfFirst2 <= 0) {
			sizeOfFirst2 += otherSegmentSize;
			seg2 = other.segments[varSegIndex2++];
		}

		int offset1 = segmentSize - sizeOfFirst1;
		int offset2 = otherSegmentSize - sizeOfFirst2;

		int needCompare = Math.min(Math.min(sizeOfFirst1, sizeOfFirst2), len);

		while (needCompare > 0) {
			// compare in one segment.
			for (int i = 0; i < needCompare; i++) {
				int res = (seg1.get(offset1 + i) & 0xFF) - (seg2.get(offset2 + i) & 0xFF);
				if (res != 0) {
					return res;
				}
			}
			if (needCompare == len) {
				break;
			}
			len -= needCompare;
			// next segment
			if (sizeOfFirst1 < sizeOfFirst2) { //I am smaller
				seg1 = segments[varSegIndex1++];
				offset1 = 0;
				offset2 += needCompare;
				sizeOfFirst1 = segmentSize;
				sizeOfFirst2 -= needCompare;
			} else if (sizeOfFirst1 > sizeOfFirst2) { //other is smaller
				seg2 = other.segments[varSegIndex2++];
				offset2 = 0;
				offset1 += needCompare;
				sizeOfFirst2 = otherSegmentSize;
				sizeOfFirst1 -= needCompare;
			} else { // same, should go ahead both.
				seg1 = segments[varSegIndex1++];
				seg2 = other.segments[varSegIndex2++];
				offset1 = 0;
				offset2 = 0;
				sizeOfFirst1 = segmentSize;
				sizeOfFirst2 = otherSegmentSize;
			}
			needCompare = Math.min(Math.min(sizeOfFirst1, sizeOfFirst2), len);
		}

		checkArgument(needCompare == len);

		return sizeInBytes - other.sizeInBytes;
	}
}
