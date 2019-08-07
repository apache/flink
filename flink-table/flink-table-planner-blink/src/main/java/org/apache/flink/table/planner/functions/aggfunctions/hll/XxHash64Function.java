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

package org.apache.flink.table.planner.functions.aggfunctions.hll;

import org.apache.flink.core.memory.HeapMemorySegment;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Compute hash value based on {@link XXH64}.
 */
public class XxHash64Function {

	public static final long DEFAULT_SEED = 42L;

	public static long hashInt(int i, long seed) {
		return XXH64.hashInt(i, seed);
	}

	public static long hashLong(long l, long seed) {
		return XXH64.hashLong(l, seed);
	}

	public static long hashUnsafeBytes(MemorySegment base, int offset, int length, long seed) {
		return XXH64.hashUnsafeBytes(base, offset, length, seed);
	}

	/**
	 * Computes hash of a given `value`. The caller needs to check the validity
	 * of input `value`.
	 */
	public static long hash(Object value, long seed) {
		if (value == null) {
			return seed;
		}

		if (value instanceof Boolean) {
			return hashInt((Boolean) value ? 1 : 0, seed);
		} else if (value instanceof Byte) {
			return hashInt((Byte) value, seed);
		} else if (value instanceof Short) {
			return hashInt((Short) value, seed);
		} else if (value instanceof Integer) {
			return hashInt((Integer) value, seed);
		} else if (value instanceof Long) {
			return hashLong((Long) value, seed);
		} else if (value instanceof Float) {
			return hashInt(Float.floatToIntBits((Float) value), seed);
		} else if (value instanceof Double) {
			return hashLong(Double.doubleToLongBits((Double) value), seed);
		} else if (value instanceof Date) {
			return hashLong(((Date) value).getTime(), seed);
		} else if (value instanceof Time) {
			return hashLong(((Time) value).getTime(), seed);
		} else if (value instanceof Timestamp) {
			return hashLong(((Timestamp) value).getTime(), seed);
		} else if (value instanceof BigDecimal) {
			BigDecimal bd = (BigDecimal) value;
			if (bd.precision() <= Decimal.MAX_LONG_DIGITS) {
				return hashLong(bd.unscaledValue().longValueExact(), seed);
			} else {
				byte[] bytes = bd.unscaledValue().toByteArray();
				HeapMemorySegment hms = HeapMemorySegment.FACTORY.wrap(bytes);
				return hashUnsafeBytes(hms, 0, hms.size(), seed);
			}
		} else if (value instanceof BinaryString) {
			BinaryString bs = (BinaryString) value;
			MemorySegment[] segments = bs.getSegments();
			if (segments.length == 1) {
				return hashUnsafeBytes(segments[0], bs.getOffset(), bs.getSizeInBytes(), seed);
			} else {
				return hashUnsafeBytes(MemorySegmentFactory.wrap(bs.getBytes()), 0, bs.getSizeInBytes(), seed);
			}
		} else {
			throw new TableException(
					"Approximate Count Distinct aggregate execution does not support type: '" + value + "'.\n" +
							"Please re-check the data type.");
		}
	}
}
