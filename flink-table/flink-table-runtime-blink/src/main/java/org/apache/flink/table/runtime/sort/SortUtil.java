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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.common.typeutils.base.NormalizedKeyUtil;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;

/**
 * Util for data formats.
 */
public class SortUtil {

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

	public static void putCharNormalizedKey(char value, MemorySegment target, int offset, int numBytes) {
		NormalizedKeyUtil.putCharNormalizedKey(value, target, offset, numBytes);
	}
}
