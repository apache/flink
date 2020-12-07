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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.base.NormalizedKeyUtil;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;

import java.nio.ByteOrder;

import static org.apache.flink.core.memory.MemoryUtils.UNSAFE;

/**
 * Util for sort.
 */
public class SortUtil {

	private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
	private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
	private static final int LONG_BYTES = 8;

	public static void minNormalizedKey(MemorySegment target, int offset, int numBytes) {
		//write min value.
		for (int i = 0; i < numBytes; i++) {
			target.put(offset + i, (byte) 0);
		}
	}

	/**
	 * Max unsigned byte is -1.
	 */
	public static void maxNormalizedKey(MemorySegment target, int offset, int numBytes) {
		//write max value.
		for (int i = 0; i < numBytes; i++) {
			target.put(offset + i, (byte) -1);
		}
	}

	public static void putShortNormalizedKey(short value, MemorySegment target, int offset,
			int numBytes) {
		NormalizedKeyUtil.putShortNormalizedKey(value, target, offset, numBytes);
	}

	public static void putByteNormalizedKey(byte value, MemorySegment target, int offset,
			int numBytes) {
		NormalizedKeyUtil.putByteNormalizedKey(value, target, offset, numBytes);
	}

	public static void putBooleanNormalizedKey(boolean value, MemorySegment target, int offset,
			int numBytes) {
		NormalizedKeyUtil.putBooleanNormalizedKey(value, target, offset, numBytes);
	}

	/**
	 * UTF-8 supports bytes comparison.
	 */
	public static void putStringNormalizedKey(
			StringData value, MemorySegment target, int offset, int numBytes) {
		BinaryStringData binaryString = (BinaryStringData) value;
		final int limit = offset + numBytes;
		final int end = binaryString.getSizeInBytes();
		for (int i = 0; i < end && offset < limit; i++) {
			target.put(offset++, binaryString.byteAt(i));
		}

		for (int i = offset; i < limit; i++) {
			target.put(i, (byte) 0);
		}
	}

	/**
	 * Just support the compact precision decimal.
	 */
	public static void putDecimalNormalizedKey(
			DecimalData record, MemorySegment target, int offset, int len) {
		assert record.isCompact();
		putLongNormalizedKey(record.toUnscaledLong(), target, offset, len);
	}

	public static void putIntNormalizedKey(int value, MemorySegment target, int offset, int numBytes) {
		NormalizedKeyUtil.putIntNormalizedKey(value, target, offset, numBytes);
	}

	public static void putLongNormalizedKey(long value, MemorySegment target, int offset,
			int numBytes) {
		NormalizedKeyUtil.putLongNormalizedKey(value, target, offset, numBytes);
	}

	/**
	 * See http://stereopsis.com/radix.html for more details.
	 */
	public static void putFloatNormalizedKey(float value, MemorySegment target, int offset,
			int numBytes) {
		int iValue = Float.floatToIntBits(value);
		iValue ^= ((iValue >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);
		NormalizedKeyUtil.putUnsignedIntegerNormalizedKey(iValue, target, offset, numBytes);
	}

	/**
	 * See http://stereopsis.com/radix.html for more details.
	 */
	public static void putDoubleNormalizedKey(double value, MemorySegment target, int offset,
			int numBytes) {
		long lValue = Double.doubleToLongBits(value);
		lValue ^= ((lValue >> (Long.SIZE - 1)) | Long.MIN_VALUE);
		NormalizedKeyUtil.putUnsignedLongNormalizedKey(lValue, target, offset, numBytes);
	}

	public static void putBinaryNormalizedKey(
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

	/**
	 * Support the compact precision TimestampData.
	 */
	public static void putTimestampNormalizedKey(
			TimestampData value, MemorySegment target, int offset, int numBytes) {
		assert value.getNanoOfMillisecond() == 0;
		putLongNormalizedKey(value.getMillisecond(), target, offset, numBytes);
	}

	public static int compareBinary(byte[] a, byte[] b) {
		return compareBinary(a, 0, a.length, b, 0, b.length);
	}

	public static int compareBinary(
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
}
