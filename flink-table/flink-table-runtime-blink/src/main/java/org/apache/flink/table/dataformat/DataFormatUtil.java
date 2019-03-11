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

package org.apache.flink.table.dataformat;

import org.apache.flink.api.common.typeutils.base.ComparatorUtil;
import org.apache.flink.core.memory.MemorySegment;

/**
 * Util for data formats.
 */
public class DataFormatUtil {

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

	/**
	 * UTF-8 supports bytes comparison.
	 */
	public static void putBinaryStringNormalizedKey(
			BinaryString value, MemorySegment target, int offset, int numBytes) {
		final int limit = offset + numBytes;
		final int end = value.getSizeInBytes();
		for (int i = 0; i < end && offset < limit; i++) {
			target.put(offset++, value.getByte(i));
		}

		for (int i = offset; i < limit; i++) {
			target.put(i, (byte) 0);
		}
	}

	/**
	 * Just support the compact precision decimal.
	 */
	public static void putDecimalNormalizedKey(
			Decimal record, MemorySegment target, int offset, int len) {
		assert record.getPrecision() <= Decimal.MAX_COMPACT_PRECISION;
		putLongNormalizedKey(record.toUnscaledLong(), target, offset, len);
	}

	public static void putIntNormalizedKey(int value, MemorySegment target, int offset, int numBytes) {
		ComparatorUtil.putIntNormalizedKey(value, target, offset, numBytes);
	}

	public static void putLongNormalizedKey(long value, MemorySegment target, int offset,
			int numBytes) {
		ComparatorUtil.putLongNormalizedKey(value, target, offset, numBytes);
	}

	/**
	 * See http://stereopsis.com/radix.html for more details.
	 */
	public static void putFloatNormalizedKey(float value, MemorySegment target, int offset,
			int numBytes) {
		int iValue = Float.floatToIntBits(value);
		iValue ^= ((iValue >> (Integer.SIZE - 1)) | Integer.MIN_VALUE);
		ComparatorUtil.putUnsignedIntegerNormalizedKey(iValue, target, offset, numBytes);
	}

	/**
	 * See http://stereopsis.com/radix.html for more details.
	 */
	public static void putDoubleNormalizedKey(double value, MemorySegment target, int offset,
			int numBytes) {
		long lValue = Double.doubleToLongBits(value);
		lValue ^= ((lValue >> (Long.SIZE - 1)) | Long.MIN_VALUE);
		ComparatorUtil.putUnsignedLongNormalizedKey(lValue, target, offset, numBytes);
	}

	public static void putCharNormalizedKey(char value, MemorySegment target, int offset, int numBytes) {
		ComparatorUtil.putCharNormalizedKey(value, target, offset, numBytes);
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
}
