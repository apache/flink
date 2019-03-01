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

	public static void copyToUnsafe(
			MemorySegment[] segments, int offset,
			Object target, int pointer, int numBytes) {
		if (inFirstSegment(segments, offset, numBytes)) {
			segments[0].copyToUnsafe(offset, target, pointer, numBytes);
		} else {
			copyMultiSegmentsToUnsafe(segments, offset, target, pointer, numBytes);
		}
	}

	private static void copyMultiSegmentsToUnsafe(
			MemorySegment[] segments, int offset,
			Object target, int pointer, int numBytes) {
		int remainSize = numBytes;
		for (MemorySegment segment : segments) {
			int remain = segment.size() - offset;
			if (remain > 0) {
				int nCopy = Math.min(remain, remainSize);
				segment.copyToUnsafe(offset, target, numBytes - remainSize + pointer, nCopy);
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

	/**
	 * unset bit from segments.
	 *
	 * @param segments target segments.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static void bitUnSet(MemorySegment[] segments, int baseOffset, int index) {
		if (segments.length == 1) {
			MemorySegment segment = segments[0];
			int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
			byte current = segment.get(offset);
			current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
			segment.put(offset, current);
		} else {
			bitUnSetMultiSegments(segments, baseOffset, index);
		}
	}

	private static void bitUnSetMultiSegments(MemorySegment[] segments, int baseOffset, int index) {
		int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		MemorySegment segment = segments[segIndex];

		byte current = segment.get(segOffset);
		current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(segOffset, current);
	}

	/**
	 * set bit from segments.
	 *
	 * @param segments target segments.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static void bitSet(MemorySegment[] segments, int baseOffset, int index) {
		if (segments.length == 1) {
			int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
			MemorySegment segment = segments[0];
			byte current = segment.get(offset);
			current |= (1 << (index & BIT_BYTE_INDEX_MASK));
			segment.put(offset, current);
		} else {
			bitSetMultiSegments(segments, baseOffset, index);
		}
	}

	private static void bitSetMultiSegments(MemorySegment[] segments, int baseOffset, int index) {
		int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		MemorySegment segment = segments[segIndex];

		byte current = segment.get(segOffset);
		current |= (1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(segOffset, current);
	}

	/**
	 * read bit from segments.
	 *
	 * @param segments target segments.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static boolean bitGet(MemorySegment[] segments, int baseOffset, int index) {
		int offset = baseOffset + ((index & BIT_BYTE_POSITION_MASK) >>> 3);
		byte current = getByte(segments, offset);
		return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
	}

	/**
	 * get boolean from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static boolean getBoolean(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 1)) {
			return segments[0].getBoolean(offset);
		} else {
			return getBooleanMultiSegments(segments, offset);
		}
	}

	private static boolean getBooleanMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		return segments[segIndex].getBoolean(segOffset);
	}

	/**
	 * set boolean from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setBoolean(MemorySegment[] segments, int offset, boolean value) {
		if (inFirstSegment(segments, offset, 1)) {
			segments[0].putBoolean(offset, value);
		} else {
			setBooleanMultiSegments(segments, offset, value);
		}
	}

	private static void setBooleanMultiSegments(MemorySegment[] segments, int offset, boolean value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		segments[segIndex].putBoolean(segOffset, value);
	}

	/**
	 * get byte from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static byte getByte(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 1)) {
			return segments[0].get(offset);
		} else {
			return getByteMultiSegments(segments, offset);
		}
	}

	private static byte getByteMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		return segments[segIndex].get(segOffset);
	}

	/**
	 * set byte from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setByte(MemorySegment[] segments, int offset, byte value) {
		if (inFirstSegment(segments, offset, 1)) {
			segments[0].put(offset, value);
		} else {
			setByteMultiSegments(segments, offset, value);
		}
	}

	private static void setByteMultiSegments(MemorySegment[] segments, int offset, byte value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		segments[segIndex].put(segOffset, value);
	}

	/**
	 * get int from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static int getInt(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 4)) {
			return segments[0].getInt(offset);
		} else {
			return getIntMultiSegments(segments, offset);
		}
	}

	private static int getIntMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			return segments[segIndex].getInt(segOffset);
		} else {
			return getIntSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	private static int getIntSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset) {
		MemorySegment segment = segments[segNum];
		int ret = 0;
		for (int i = 0; i < 4; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			int unsignedByte = segment.get(segOffset) & 0xff;
			if (LITTLE_ENDIAN) {
				ret |= (unsignedByte << (i * 8));
			} else {
				ret |= (unsignedByte << ((3 - i) * 8));
			}
			segOffset++;
		}
		return ret;
	}

	/**
	 * set int from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setInt(MemorySegment[] segments, int offset, int value) {
		if (inFirstSegment(segments, offset, 4)) {
			segments[0].putInt(offset, value);
		} else {
			setIntMultiSegments(segments, offset, value);
		}
	}

	private static void setIntMultiSegments(MemorySegment[] segments, int offset, int value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			segments[segIndex].putInt(segOffset, value);
		} else {
			setIntSlowly(segments, segSize, segIndex, segOffset, value);
		}
	}

	private static void setIntSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset, int value) {
		MemorySegment segment = segments[segNum];
		for (int i = 0; i < 4; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			int unsignedByte;
			if (LITTLE_ENDIAN) {
				unsignedByte = value >> (i * 8);
			} else {
				unsignedByte = value >> ((3 - i) * 8);
			}
			segment.put(segOffset, (byte) unsignedByte);
			segOffset++;
		}
	}

	/**
	 * get long from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static long getLong(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 8)) {
			return segments[0].getLong(offset);
		} else {
			return getLongMultiSegments(segments, offset);
		}
	}

	private static long getLongMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			return segments[segIndex].getLong(segOffset);
		} else {
			return getLongSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	private static long getLongSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset) {
		MemorySegment segment = segments[segNum];
		long ret = 0;
		for (int i = 0; i < 8; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			long unsignedByte = segment.get(segOffset) & 0xff;
			if (LITTLE_ENDIAN) {
				ret |= (unsignedByte << (i * 8));
			} else {
				ret |= (unsignedByte << ((7 - i) * 8));
			}
			segOffset++;
		}
		return ret;
	}

	/**
	 * set long from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setLong(MemorySegment[] segments, int offset, long value) {
		if (inFirstSegment(segments, offset, 8)) {
			segments[0].putLong(offset, value);
		} else {
			setLongMultiSegments(segments, offset, value);
		}
	}

	private static void setLongMultiSegments(MemorySegment[] segments, int offset, long value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			segments[segIndex].putLong(segOffset, value);
		} else {
			setLongSlowly(segments, segSize, segIndex, segOffset, value);
		}
	}

	private static void setLongSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset, long value) {
		MemorySegment segment = segments[segNum];
		for (int i = 0; i < 8; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			long unsignedByte;
			if (LITTLE_ENDIAN) {
				unsignedByte = value >> (i * 8);
			} else {
				unsignedByte = value >> ((7 - i) * 8);
			}
			segment.put(segOffset, (byte) unsignedByte);
			segOffset++;
		}
	}

	/**
	 * get short from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static short getShort(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 2)) {
			return segments[0].getShort(offset);
		} else {
			return getShortMultiSegments(segments, offset);
		}
	}

	private static short getShortMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 1) {
			return segments[segIndex].getShort(segOffset);
		} else {
			return (short) getTwoByteSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	/**
	 * set short from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setShort(MemorySegment[] segments, int offset, short value) {
		if (inFirstSegment(segments, offset, 2)) {
			segments[0].putShort(offset, value);
		} else {
			setShortMultiSegments(segments, offset, value);
		}
	}

	private static void setShortMultiSegments(MemorySegment[] segments, int offset, short value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 1) {
			segments[segIndex].putShort(segOffset, value);
		} else {
			setTwoByteSlowly(segments, segSize, segIndex, segOffset, value, value >> 8);
		}
	}

	/**
	 * get float from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static float getFloat(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 4)) {
			return segments[0].getFloat(offset);
		} else {
			return getFloatMultiSegments(segments, offset);
		}
	}

	private static float getFloatMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			return segments[segIndex].getFloat(segOffset);
		} else {
			return Float.intBitsToFloat(getIntSlowly(segments, segSize, segIndex, segOffset));
		}
	}

	/**
	 * set float from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setFloat(MemorySegment[] segments, int offset, float value) {
		if (inFirstSegment(segments, offset, 4)) {
			segments[0].putFloat(offset, value);
		} else {
			setFloatMultiSegments(segments, offset, value);
		}
	}

	private static void setFloatMultiSegments(MemorySegment[] segments, int offset, float value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			segments[segIndex].putFloat(segOffset, value);
		} else {
			setIntSlowly(segments, segSize, segIndex, segOffset, Float.floatToRawIntBits(value));
		}
	}

	/**
	 * get double from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static double getDouble(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 8)) {
			return segments[0].getDouble(offset);
		} else {
			return getDoubleMultiSegments(segments, offset);
		}
	}

	private static double getDoubleMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			return segments[segIndex].getDouble(segOffset);
		} else {
			return Double.longBitsToDouble(getLongSlowly(segments, segSize, segIndex, segOffset));
		}
	}

	/**
	 * set double from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setDouble(MemorySegment[] segments, int offset, double value) {
		if (inFirstSegment(segments, offset, 8)) {
			segments[0].putDouble(offset, value);
		} else {
			setDoubleMultiSegments(segments, offset, value);
		}
	}

	private static void setDoubleMultiSegments(MemorySegment[] segments, int offset, double value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			segments[segIndex].putDouble(segOffset, value);
		} else {
			setLongSlowly(segments, segSize, segIndex, segOffset, Double.doubleToRawLongBits(value));
		}
	}

	/**
	 * get char from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static char getChar(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 2)) {
			return segments[0].getChar(offset);
		} else {
			return getCharMultiSegments(segments, offset);
		}
	}

	private static char getCharMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 1) {
			return segments[segIndex].getChar(segOffset);
		} else {
			return (char) getTwoByteSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	private static int getTwoByteSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset) {
		MemorySegment segment = segments[segNum];
		int ret = 0;
		for (int i = 0; i < 2; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			int unsignedByte = segment.get(segOffset) & 0xff;
			if (LITTLE_ENDIAN) {
				ret |= (unsignedByte << (i * 8));
			} else {
				ret |= (unsignedByte << ((1 - i) * 8));
			}
			segOffset++;
		}
		return ret;
	}

	/**
	 * set char from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setChar(MemorySegment[] segments, int offset, char value) {
		if (inFirstSegment(segments, offset, 2)) {
			segments[0].putChar(offset, value);
		} else {
			setCharMultiSegments(segments, offset, value);
		}
	}

	private static void setCharMultiSegments(MemorySegment[] segments, int offset, char value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			segments[segIndex].putChar(segOffset, value);
		} else {
			setTwoByteSlowly(segments, segSize, segIndex, segOffset, value, value >> 8);
		}
	}

	private static void setTwoByteSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset, int b1, int b2) {
		MemorySegment segment = segments[segNum];
		segment.put(segOffset, (byte) (LITTLE_ENDIAN ? b1 : b2));
		segOffset++;
		if (segOffset == segSize) {
			segment = segments[++segNum];
			segOffset = 0;
		}
		segment.put(segOffset, (byte) (LITTLE_ENDIAN ? b2 : b1));
	}
}
