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

import java.math.BigDecimal;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Sql Decimal value based on a Long.
 */
public class CompactDecimal extends Decimal {

	final long longVal;

	public CompactDecimal(int precision, int scale, long longVal) {
		super(precision, scale);
		this.longVal = longVal;
	}

	@Override
	public boolean isCompact() {
		return true;
	}

	private BigDecimal cachedBigDecimal = null;

	public BigDecimal toBigDecimal() {
		if (cachedBigDecimal == null) {
			cachedBigDecimal =  BigDecimal.valueOf(longVal, scale);
		}
		return cachedBigDecimal;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(longVal);
	}

	@Override
	public boolean equals(final Object o) {
		if (!(o instanceof CompactDecimal)) {
			return false;
		}
		CompactDecimal that = (CompactDecimal) o;
		if (this.scale == that.scale) {
			return this.longVal == that.longVal;
		}
		return this.toBigDecimal().compareTo(that.toBigDecimal()) == 0;
	}

	@Override
	public int signum() {
		return Long.signum(longVal);
	}

	// convert long to Decimal.
	// long vlaue `l` must have at most `precision` digits.
	// the decimal result is `l / POW10[scale]`
	public static CompactDecimal fromLong(long l, int precision, int scale) {
		checkArgument(precision > 0 && precision <= MAX_LONG_DIGITS);
		checkArgument((l >= 0 ? l : -l) < POW10[precision]);
		return new CompactDecimal(precision, scale, l);
	}

	@Override
	public CompactDecimal copy() {
		return new CompactDecimal(precision, scale, longVal);
	}

	public long toUnscaledLong() {
		assert isCompact();
		return longVal;
	}

	public static CompactDecimal fromUnscaledLong(int precision, int scale, long longVal) {
		return new CompactDecimal(precision, scale, longVal);
	}

	@Override
	public byte[] toUnscaledBytes() {
		// big endian; consistent with BigInteger.toByteArray()
		byte[] bytes = new byte[8];
		long l = longVal;
		for (int i = 0; i < 8; i++) {
			bytes[7 - i] = (byte) l;
			l >>>= 8;
		}
		return bytes;
	}

	@Override
	public double doubleValue() {
		return ((double) longVal) / POW10[scale];
	}

	@Override
	public Decimal negate() {
		return new CompactDecimal(precision, scale, -longVal);
	}

	@Override
	public Decimal abs() {
		if (longVal >= 0) {
			return this;
		} else {
			return new CompactDecimal(precision, scale, -longVal);
		}
	}
}
