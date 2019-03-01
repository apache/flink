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

package org.apache.flink.table.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;

import java.nio.ByteOrder;

/**
 * Util for data format segments calc.
 */
public class SegmentsUtil {

	/**
	 * Constant that flags the byte order.
	 */
	public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

	private static final int BIT_BYTE_POSITION_MASK = 0xfffffff8;

	private static final int BIT_BYTE_INDEX_MASK = 7;

	/**
	 * Copy segments to a new byte[].
	 *
	 * @param segments Source segments.
	 * @param offset Source segments offset.
	 * @param numBytes the number bytes to copy.
	 */
	public static byte[] copyToBytes(MemorySegment[] segments, int offset, int numBytes) {
		return copyToBytes(segments, offset, new byte[numBytes], 0, numBytes);
	}

	/**
	 * Copy segments to target byte[].
	 *
	 * @param segments Source segments.
	 * @param offset Source segments offset.
	 * @param bytes target byte[].
	 * @param bytesOffset target byte[] offset.
	 * @param numBytes the number bytes to copy.
	 */
	public static byte[] copyToBytes(MemorySegment[] segments, int offset, byte[] bytes,
			int bytesOffset, int numBytes) {
		if (inFirstSegment(segments, offset, numBytes)) {
			segments[0].get(offset, bytes, bytesOffset, numBytes);
		} else {
			copyMultiSegmentsToBytes(segments, offset, bytes, bytesOffset, numBytes);
		}
		return bytes;
	}

	private static void copyMultiSegmentsToBytes(MemorySegment[] segments, int offset, byte[] bytes,
			int bytesOffset, int numBytes) {
		int remainSize = numBytes;
		for (MemorySegment segment : segments) {
			int remain = segment.size() - offset;
			if (remain > 0) {
				int nCopy = Math.min(remain, remainSize);
				segment.get(offset, bytes, numBytes - remainSize + bytesOffset, nCopy);
				remainSize -= nCopy;
				// next new segment.
				offset = 0;
				if (remainSize == 0) {
					return;
				}
			} else {
				// remain is negative, let's advance to next segment
				// now the offset = offset - segmentSize (-remain)
				offset = -remain;
			}
		}
	}

	/**
	 * Equals two memory segments regions.
	 *
	 * @param segments1 Segments 1
	 * @param offset1 Offset of segments1 to start equaling
	 * @param segments2 Segments 2
	 * @param offset2 Offset of segments2 to start equaling
	 * @param len Length of the equaled memory region
	 *
	 * @return true if equal, false otherwise
	 */
	public static boolean equals(
			MemorySegment[] segments1, int offset1,
			MemorySegment[] segments2, int offset2, int len) {
		if (inFirstSegment(segments1, offset1, len) && inFirstSegment(segments2, offset2, len)) {
			return segments1[0].equalTo(segments2[0], offset1, offset2, len);
		} else {
			return equalsMultiSegments(segments1, offset1, segments2, offset2, len);
		}
	}

	@VisibleForTesting
	static boolean equalsMultiSegments(
			MemorySegment[] segments1, int offset1,
			MemorySegment[] segments2, int offset2, int len) {
		if (len == 0) {
			// quick way and avoid segSize is zero.
			return true;
		}

		int segSize1 = segments1[0].size();
		int segSize2 = segments2[0].size();

		// find first segIndex and segOffset of segments.
		int segIndex1 = offset1 / segSize1;
		int segIndex2 = offset2 / segSize2;
		int segOffset1 = offset1 - segSize1 * segIndex1; // equal to %
		int segOffset2 = offset2 - segSize2 * segIndex2; // equal to %

		while (len > 0) {
			int equalLen = Math.min(Math.min(len, segSize1 - segOffset1), segSize2 - segOffset2);
			if (!segments1[segIndex1].equalTo(segments2[segIndex2], segOffset1, segOffset2, equalLen)) {
				return false;
			}
			len -= equalLen;
			segOffset1 += equalLen;
			if (segOffset1 == segSize1) {
				segOffset1 = 0;
				segIndex1++;
			}
			segOffset2 += equalLen;
			if (segOffset2 == segSize2) {
				segOffset2 = 0;
				segIndex2++;
			}
		}
		return true;
	}

	/**
	 * Is it just in first MemorySegment, we use quick way to do something.
	 */
	private static boolean inFirstSegment(MemorySegment[] segments, int offset, int numBytes) {
		return numBytes + offset <= segments[0].size();
	}

	/**
	 * unset bit.
	 *
	 * @param segment target segment.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static void bitUnSet(MemorySegment segment, int baseOffset, int index) {
		int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
		byte current = segment.get(offset);
		current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(offset, current);
	}

	/**
	 * set bit.
	 *
	 * @param segment target segment.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static void bitSet(MemorySegment segment, int baseOffset, int index) {
		int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
		byte current = segment.get(offset);
		current |= (1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(offset, current);
	}

	/**
	 * read bit.
	 *
	 * @param segment target segment.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static boolean bitGet(MemorySegment segment, int baseOffset, int index) {
		int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
		byte current = segment.get(offset);
		return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
	}
}
