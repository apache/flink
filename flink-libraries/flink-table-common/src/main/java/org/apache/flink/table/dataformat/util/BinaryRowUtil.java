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

package org.apache.flink.table.dataformat.util;

import org.apache.flink.api.common.typeutils.base.ComparatorUtil;
import org.apache.flink.api.common.typeutils.base.DateComparator;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemoryUtils;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.util.hash.Murmur32;

import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.flink.table.util.hash.Murmur32.DEFAULT_SEED;

/**
 * Util for binary row. Many of the methods in this class are used in code generation.
 * So ignore IDE warnings.
 */
public class BinaryRowUtil {

	public static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;
	public static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
	public static final int BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
	public static final int SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
	public static final int INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
	public static final int LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
	public static final int FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
	public static final int DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);
	public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
	private static final int LONG_BYTES = 8;

	public static final BinaryRow EMPTY_ROW = new BinaryRow(0);

	static {
		int size = EMPTY_ROW.getFixedLengthPartSize();
		byte[] bytes = new byte[size];
		EMPTY_ROW.pointTo(MemorySegmentFactory.wrap(bytes), 0, size);
	}

	public static int compareBoolean(boolean a, boolean b) {
		return Boolean.compare(a, b);
	}

	public static int compareByte(byte a, byte b) {
		return Byte.compare(a, b);
	}

	public static int compareShort(short a, short b) {
		return Short.compare(a, b);
	}

	public static int compareInt(int a, int b) {
		return Integer.compare(a, b);
	}

	public static int compareLong(long a, long b) {
		return Long.compare(a, b);
	}

	public static int compareFloat(float a, float b) {
		return Float.compare(a, b);
	}

	public static int compareDouble(double a, double b) {
		return Double.compare(a, b);
	}

	public static int compareChar(char a, char b) {
		return Character.compare(a, b);
	}

	public static int compareBinaryString(BinaryString a, BinaryString b) {
		return a.compareTo(b);
	}

	public static int compareDecimal(Decimal a, Decimal b) {
		return a.compareTo(b);
	}

	public static int compareDate(Date a, Date b) {
		return a.compareTo(b);
	}

	public static int compareTime(Time a, Time b) {
		return a.compareTo(b);
	}

	public static int compareTimestamp(Timestamp a, Timestamp b) {
		return a.compareTo(b);
	}

	public static int compareByteArray(byte[] a, byte[] b) {
		return compareByteArray(a, 0, a.length, b, 0, b.length);
	}

	public static int compareByteArray(
			byte[] buffer1, int offset1, int length1,
			byte[] buffer2, int offset2, int length2) {
		// Short circuit equal case
		if (buffer1 == buffer2 &&
				offset1 == offset2 &&
				length1 == length2) {
			return 0;
		}
		int minLength = Math.min(length1, length2);
		int minWords = minLength / LONG_BYTES;
		int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
		int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

        /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit.
         * On the other hand, it is substantially faster on 64-bit.
         */
		for (int i = 0; i < minWords * LONG_BYTES; i += LONG_BYTES) {
			long lw = UNSAFE.getLong(buffer1, offset1Adj + (long) i);
			long rw = UNSAFE.getLong(buffer2, offset2Adj + (long) i);
			long diff = lw ^ rw;

			if (diff != 0) {
				if (!LITTLE_ENDIAN) {
					return lessThanUnsigned(lw, rw) ? -1 : 1;
				}

				// Use binary search
				int n = 0;
				int y;
				int x = (int) diff;
				if (x == 0) {
					x = (int) (diff >>> 32);
					n = 32;
				}

				y = x << 16;
				if (y == 0) {
					n += 16;
				} else {
					x = y;
				}

				y = x << 8;
				if (y == 0) {
					n += 8;
				}
				return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
			}
		}

		// The epilogue to cover the last (minLength % 8) elements.
		for (int i = minWords * LONG_BYTES; i < minLength; i++) {
			int result = unsignedByteToInt(buffer1[offset1 + i]) -
					unsignedByteToInt(buffer2[offset2 + i]);
			if (result != 0) {
				return result;
			}
		}
		return length1 - length2;
	}

	private static int unsignedByteToInt(byte value) {
		return value & 0xff;
	}

	private static boolean lessThanUnsigned(long x1, long x2) {
		return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
	}

	public static int hashInt(int value) {
		return Integer.hashCode(value);
	}

	public static int hashLong(long value) {
		return Long.hashCode(value);
	}

	public static int hashShort(short value) {
		return Short.hashCode(value);
	}

	public static int hashByte(byte value) {
		return Byte.hashCode(value);
	}

	public static int hashFloat(float value) {
		return Float.hashCode(value);
	}

	public static int hashDouble(double value) {
		return Double.hashCode(value);
	}

	public static int hashBoolean(boolean value) {
		return Boolean.hashCode(value);
	}

	public static int hashChar(char value) {
		return Character.hashCode(value);
	}

	public static int hashObject(Object value) {
		return value.hashCode();
	}

	public static int hashString(BinaryString value) {
		return value.hashCode();
	}

	public static int hashDecimal(Decimal value) {
		return value.hashCode();
	}

	public static int hashByteArray(byte[] value) {
		return Murmur32.hashUnsafeBytes(value, BYTE_ARRAY_BASE_OFFSET, value.length, DEFAULT_SEED);
	}

	public static long hashByteArray64(byte[] value) {
		return Murmur32.hashUnsafe64(value, BYTE_ARRAY_BASE_OFFSET, value.length, DEFAULT_SEED);
	}

	public static long hashDecimal64(Decimal value) {
		// TODO real hash64.
		return value.hashCode();
	}

	public static void minNormalizedKey(MemorySegment target, int offset, int numBytes) {
		//write min value.
		for (int i = 0; i < numBytes; i++) {
			target.put(offset + i, (byte) 0);
		}
	}

	public static void maxNormalizedKey(MemorySegment target, int offset, int numBytes) {
		//write max value.
		for (int i = 0; i < numBytes; i++) {
			target.put(offset + i, (byte) -1);
		}
	}

	public static void putShortNormalizedKey(short value, MemorySegment target, int offset,
			int numBytes) {
		ComparatorUtil.putShortNormalizedKey(value, target, offset, numBytes);
	}

	public static void putByteNormalizedKey(byte value, MemorySegment target, int offset,
			int numBytes) {
		ComparatorUtil.putByteNormalizedKey(value, target, offset, numBytes);
	}

	public static void putBooleanNormalizedKey(boolean value, MemorySegment target, int offset,
			int numBytes) {
		ComparatorUtil.putBooleanNormalizedKey(value, target, offset, numBytes);
	}

	public static void putBinaryStringNormalizedKey(
			BinaryString value, MemorySegment target, int offset, int numBytes) {
		final int limit = offset + numBytes;
		final int end = value.numBytes();
		for (int i = 0; i < end && offset < limit; i++) {
			target.put(offset++, value.getByte(i));
		}

		for (int i = offset; i < limit; i++) {
			target.put(i, (byte) 0);
		}
	}

	public static void putDecimalNormalizedKey(
			Decimal record, MemorySegment target, int offset, int len) {
		assert record.getPrecision() <= Decimal.MAX_COMPACT_PRECISION;
		putLongNormalizedKey(record.toUnscaledLong(), target, offset, len);
	}

	public static void putByteArrayNormalizedKey(
			byte[] value, MemorySegment target, int offset, int numBytes) {
		final int limit = offset + numBytes;
		final int end = value.length;
		for (int i = 0; i < end && offset < limit; i++) {
			target.put(offset++, value[i]);
		}

		for (int i = offset; i < limit; i++) {
			target.put(i, (byte) 0);
		}
	}

	public static void putIntNormalizedKey(int value, MemorySegment target, int offset, int numBytes) {
		ComparatorUtil.putIntNormalizedKey(value, target, offset, numBytes);
	}

	public static void putLongNormalizedKey(long value, MemorySegment target, int offset,
			int numBytes) {
		ComparatorUtil.putLongNormalizedKey(value, target, offset, numBytes);
	}

	/**
	 * See http://stereopsis.com/radix.html/ for more details.
	 */
	public static void putFloatNormalizedKey(float value, MemorySegment target, int offset,
			int numBytes) {
		int iValue = Float.floatToIntBits(value);
		iValue ^= ((iValue >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);
		ComparatorUtil.putUnsignedIntegerNormalizedKey(iValue, target, offset, numBytes);
	}

	public static void putDoubleNormalizedKey(double value, MemorySegment target, int offset,
			int numBytes) {
		long lValue = Double.doubleToLongBits(value);
		lValue ^= ((lValue >> (Long.SIZE - 1)) | Long.MIN_VALUE);
		ComparatorUtil.putUnsignedLongNormalizedKey(lValue, target, offset, numBytes);
	}

	public static void putCharNormalizedKey(char value, MemorySegment target, int offset,
			int numBytes) {
		ComparatorUtil.putCharNormalizedKey(value, target, offset, numBytes);
	}

	public static void putDateNormalizedKey(Date value, MemorySegment target, int offset,
			int numBytes) {
		DateComparator.putNormalizedKeyDate(value, target, offset, numBytes);
	}

	public static void putTimeNormalizedKey(Time value, MemorySegment target, int offset,
			int numBytes) {
		DateComparator.putNormalizedKeyDate(value, target, offset, numBytes);
	}

	public static void putTimestampNormalizedKey(Timestamp value, MemorySegment target, int offset,
			int numBytes) {
		// put Date key
		DateComparator.putNormalizedKeyDate(value, target, offset, numBytes > 8 ? 8 : numBytes);
		numBytes -= 8;
		offset += 8;
		if (numBytes <= 0) {
			// nothing to do
		}
		// put nanos
		else if (numBytes < 4) {
			final int nanos = value.getNanos();
			for (int i = 0; numBytes > 0; numBytes--, i++) {
				target.put(offset + i, (byte) (nanos >>> ((3 - i) << 3)));
			}
		}
		// put nanos with padding
		else {
			final int nanos = value.getNanos();
			target.putIntBigEndian(offset, nanos);
			for (int i = 4; i < numBytes; i++) {
				target.put(offset + i, (byte) 0);
			}
		}
	}

	public static boolean byteArrayEquals(byte[] left, byte[] right, int length) {
		return byteArrayEquals(
				left, BYTE_ARRAY_BASE_OFFSET, right, BYTE_ARRAY_BASE_OFFSET, length);
	}

	public static boolean byteArrayEquals(
			Object left, long leftOffset, Object right, long rightOffset, int length) {
		int i = 0;

		while (i <= length - 8) {
			if (UNSAFE.getLong(left, leftOffset + i) !=
					UNSAFE.getLong(right, rightOffset + i)) {
				return false;
			}
			i += 8;
		}

		while (i < length) {
			if (UNSAFE.getByte(left, leftOffset + i) !=
					UNSAFE.getByte(right, rightOffset + i)) {
				return false;
			}
			i += 1;
		}
		return true;
	}

	public static boolean equals(
			MemorySegment[] segments1, int offset1,
			MemorySegment[] segments2, int offset2, int len) {
		if (allInFirstSeg(segments1, offset1, len) && allInFirstSeg(segments2, offset2, len)) {
			return segments1[0].equalTo(segments2[0], offset1, offset2, len);
		} else {
			return equalsSlow(segments1, offset1, segments2, offset2, len);
		}
	}

	public static boolean equalsSlow(
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

	public static byte[] copy(MemorySegment[] segments, int offset, int numBytes) {
		return copy(segments, offset, new byte[numBytes], 0, numBytes);
	}

	public static byte[] copy(MemorySegment[] segments, int offset, byte[] bytes,
			int bytesOffset, int numBytes) {
		if (allInFirstSeg(segments, offset, numBytes)) {
			segments[0].get(offset, bytes, bytesOffset, numBytes);
		} else {
			copySlow(segments, offset, bytes, bytesOffset, numBytes);
		}
		return bytes;
	}

	public static void copySlow(MemorySegment[] segments, int offset, byte[] bytes, int numBytes) {
		copySlow(segments, offset, bytes, 0, numBytes);
	}

	public static void copySlow(MemorySegment[] segments, int offset, byte[] bytes,
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
		if (segments.length == 1) {
			segments[0].copyToUnsafe(offset, target, pointer, numBytes);
		} else {
			copyToUnsafeSlow(segments, offset, target, pointer, numBytes);
		}
	}

	private static void copyToUnsafeSlow(
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

	public static void copyFromBytes(
			MemorySegment[] segments, int offset,
			byte[] target, int pointer, int numBytes) {
		copyFromUnsafe(segments, offset, target, BYTE_ARRAY_BASE_OFFSET + pointer, numBytes);
	}

	public static void copyFromUnsafe(
			MemorySegment[] segments, int offset,
			Object target, int pointer, int numBytes) {
		if (segments.length == 1) {
			segments[0].copyFromUnsafe(offset, target, pointer, numBytes);
		} else {
			copyFromUnsafeSlow(segments, offset, target, pointer, numBytes);
		}
	}

	private static void copyFromUnsafeSlow(
			MemorySegment[] segments, int offset,
			Object target, int pointer, int numBytes) {
		int remainSize = numBytes;
		for (MemorySegment segment : segments) {
			int remain = segment.size() - offset;
			if (remain > 0) {
				int nCopy = Math.min(remain, remainSize);
				segment.copyFromUnsafe(offset, target, numBytes - remainSize + pointer, nCopy);
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

	public static int getVariableLength(InternalType[] types) {
		int length = 0;
		for (InternalType type : types) {
			if (!BinaryRow.isFixedLength(type)) {
				// find a better way of computing generic type field variable-length
				// right now we use a small value assumption
				length += 16;
			}
		}
		return length;
	}

	private static boolean allInFirstSeg(MemorySegment[] segments, int offset, int numBytes) {
		return numBytes + offset <= segments[0].size();
	}

	/**
	 * Find equal segments2 in segments1.
	 * @param segments1 segs to find.
	 * @param segments2 sub segs.
	 * @return Return the found offset, return -1 if not find.
	 */
	public static int find(
			MemorySegment[] segments1, int offset1, int numBytes1,
			MemorySegment[] segments2, int offset2, int numBytes2) {
		if (numBytes2 == 0) { // quick way 1.
			return offset1;
		}
		if (allInFirstSeg(segments1, offset1, numBytes1) &&
				allInFirstSeg(segments2, offset2, numBytes2)) {
			byte first = segments2[0].get(offset2);
			int end = numBytes1 - numBytes2 + offset1;
			for (int i = offset1; i <= end; i++) {
				// quick way 2: equal first byte.
				if (segments1[0].get(i) == first &&
						segments1[0].equalTo(segments2[0], i, offset2, numBytes2)) {
					return i;
				}
			}
			return -1;
		} else {
			return findSlow(segments1, offset1, numBytes1, segments2, offset2, numBytes2);
		}
	}

	private static int findSlow(
			MemorySegment[] segments1, int offset1, int numBytes1,
			MemorySegment[] segments2, int offset2, int numBytes2) {
		int end = numBytes1 - numBytes2 + offset1;
		for (int i = offset1; i <= end; i++) {
			if (equalsSlow(segments1, i, segments2, offset2, numBytes2)) {
				return i;
			}
		}
		return -1;
	}
}
