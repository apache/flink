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

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfoFactory;
import org.apache.flink.table.runtime.util.SegmentsUtil;
import org.apache.flink.table.types.logical.DecimalType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Sql Decimal value. A mutable implementation of BigDecimal that can hold a Long if values
 * are small enough.
 *
 * <p>The semantics of the fields are as follows:
 * - precision and scale represent the SQL precision and scale we are looking for
 * - If decimalVal is set, it represents the whole decimal value
 * - Otherwise, the decimal value is longVal / (10 ** scale).
 */
@TypeInfo(DecimalTypeInfoFactory.class)
public final class Decimal implements Comparable<Decimal> {

	private static final MathContext MC_DIVIDE = new MathContext(38, RoundingMode.HALF_UP);

	public static final int MAX_COMPACT_PRECISION = 18;

	/**
	 * Maximum number of decimal digits an Int can represent. (1e9 < Int.MaxValue < 1e10)
	 */
	public static final int MAX_INT_DIGITS = 9;

	/**
	 * Maximum number of decimal digits a Long can represent. (1e18 < Long.MaxValue < 1e19)
	 */
	public static final int MAX_LONG_DIGITS = 18;

	public static final long[] POW10 = new long[MAX_COMPACT_PRECISION + 1];
	static {
		POW10[0] = 1;
		for (int i = 1; i < POW10.length; i++) {
			POW10[i] = 10 * POW10[i - 1];
		}
	}

	public static final DecimalType DECIMAL_SYSTEM_DEFAULT = new DecimalType(DecimalType.MAX_PRECISION, 18);

	// for now, we follow closely to what Spark does.
	// see if we can improve upon it later.

	// (precision, scale) is always correct.
	// if precision > MAX_COMPACT_PRECISION,
	//   `decimalVal` represents the value. `longVal` is undefined
	// otherwise, (longVal, scale) represents the value
	//   `decimalVal` may be set and cached

	private final int precision;
	private final int scale;

	private final long longVal;
	private BigDecimal decimalVal;

	// this constructor does not perform any sanity check.
	private Decimal(int precision, int scale, long longVal, BigDecimal decimalVal) {
		this.precision = precision;
		this.scale = scale;
		this.longVal = longVal;
		this.decimalVal = decimalVal;
	}

	public boolean isCompact() {
		return isCompact(this.precision);
	}

	public static boolean isCompact(int precision) {
		return precision <= MAX_COMPACT_PRECISION;
	}

	public BigDecimal toBigDecimal() {
		BigDecimal bd = decimalVal;
		if (bd == null) {
			decimalVal = bd = BigDecimal.valueOf(longVal, scale);
		}
		return bd;
	}

	@Override
	public int hashCode() {
		return toBigDecimal().hashCode();
	}

	@Override
	public int compareTo(Decimal that) {
		if (this.isCompact() && that.isCompact() && this.scale == that.scale) {
			return Long.compare(this.longVal, that.longVal);
		}
		return this.toBigDecimal().compareTo(that.toBigDecimal());
	}

	@Override
	public boolean equals(final Object o) {
		if (!(o instanceof Decimal)) {
			return false;
		}
		Decimal that = (Decimal) o;
		return this.compareTo(that) == 0;
	}

	@Override
	public String toString() {
		return toBigDecimal().toPlainString();
	}

	/**
	 * Returns the signum function of this decimal.  (The return value is -1 if this decimal
	 * is negative; 0 if this decimal is zero; and 1 if this decimal is positive.)
	 *
	 * @return the signum function of this decimal.
	 */
	public int signum() {
		if (isCompact()) {
			return Long.signum(longVal);
		} else {
			return decimalVal.signum();
		}
	}

	// convert long to Decimal.
	// long vlaue `l` must have at most `precision` digits.
	// the decimal result is `l / POW10[scale]`
	public static Decimal fromLong(long l, int precision, int scale) {
		checkArgument(precision > 0 && precision <= MAX_LONG_DIGITS);
		checkArgument((l >= 0 ? l : -l) < POW10[precision]);
		return new Decimal(precision, scale, l, null);
	}

	// convert external BigDecimal to internal representation.
	// first, value may be rounded to have the desired `scale`
	// then `precision` is checked. if precision overflow, it will return `null`
	public static Decimal fromBigDecimal(BigDecimal bd, int precision, int scale) {
		bd = bd.setScale(scale, RoundingMode.HALF_UP);
		if (bd.precision() > precision) {
			return null;
		}

		long longVal = -1;
		if (precision <= MAX_COMPACT_PRECISION) {
			longVal = bd.movePointRight(scale).longValueExact();
		}
		return new Decimal(precision, scale, longVal, bd);
	}

	public static Decimal zero(int precision, int scale) {
		if (precision <= MAX_COMPACT_PRECISION) {
			return new Decimal(precision, scale, 0, null);
		} else {
			return fromBigDecimal(BigDecimal.ZERO, precision, scale);
		}
	}

	public Decimal copy() {
		return new Decimal(precision, scale, longVal, decimalVal);
	}

	public long toUnscaledLong() {
		assert isCompact();
		return longVal;
	}

	public static Decimal fromUnscaledLong(int precision, int scale, long longVal) {
		assert isCompact(precision);
		return new Decimal(precision, scale, longVal, null);
	}

	public byte[] toUnscaledBytes() {
		if (!isCompact()) {
			return toBigDecimal().unscaledValue().toByteArray();
		}

		// big endian; consistent with BigInteger.toByteArray()
		byte[] bytes = new byte[8];
		long l = longVal;
		for (int i = 0; i < 8; i++) {
			bytes[7 - i] = (byte) l;
			l >>>= 8;
		}
		return bytes;
	}

	// we assume the bytes were generated by us from toUnscaledBytes()
	public static Decimal fromUnscaledBytes(int precision, int scale, byte[] bytes) {
		if (precision > MAX_COMPACT_PRECISION) {
			BigDecimal bd = new BigDecimal(new BigInteger(bytes), scale);
			return new Decimal(precision, scale, -1, bd);
		}
		assert bytes.length == 8;
		long l = 0;
		for (int i = 0; i < 8; i++) {
			l <<= 8;
			l |= (bytes[i] & (0xff));
		}
		return new Decimal(precision, scale, l, null);
	}

	public double doubleValue() {
		if (isCompact()) {
			return ((double) longVal) / POW10[scale];
		} else {
			return decimalVal.doubleValue();
		}
	}

	public Decimal negate() {
		if (isCompact()) {
			return new Decimal(precision, scale, -longVal, null);
		} else {
			return new Decimal(precision, scale, -1, decimalVal.negate());
		}
	}

	public Decimal abs() {
		if (isCompact()) {
			if (longVal >= 0) {
				return this;
			} else {
				return new Decimal(precision, scale, -longVal, null);
			}
		} else {
			if (decimalVal.signum() >= 0) {
				return this;
			} else {
				return new Decimal(precision, scale, -1, decimalVal.negate());
			}
		}
	}

	// floor()/ceil() preserve precision, but set scale to 0.
	// note that result may exceed the original precision.

	public Decimal floor() {
		BigDecimal bd = toBigDecimal().setScale(0, RoundingMode.FLOOR);
		return fromBigDecimal(bd, bd.precision(), 0);
	}

	public Decimal ceil() {
		BigDecimal bd = toBigDecimal().setScale(0, RoundingMode.CEILING);
		return fromBigDecimal(bd, bd.precision(), 0);
	}

	public int getPrecision() {
		return precision;
	}

	public int getScale() {
		return scale;
	}

	public static Decimal add(Decimal v1, Decimal v2, int precision, int scale) {
		if (v1.isCompact() && v2.isCompact() && v1.scale == v2.scale) {
			assert scale == v1.scale; // no need to rescale
			try {
				long ls = Math.addExact(v1.longVal, v2.longVal); // checks overflow
				return new Decimal(precision, scale, ls, null);
			} catch (ArithmeticException e) {
				// overflow, fall through
			}
		}
		BigDecimal bd = v1.toBigDecimal().add(v2.toBigDecimal());
		return fromBigDecimal(bd, precision, scale);
	}

	public static Decimal subtract(Decimal v1, Decimal v2, int precision, int scale) {
		if (v1.isCompact() && v2.isCompact() && v1.scale == v2.scale) {
			assert scale == v1.scale; // no need to rescale
			try {
				long ls = Math.subtractExact(v1.longVal, v2.longVal); // checks overflow
				return new Decimal(precision, scale, ls, null);
			} catch (ArithmeticException e) {
				// overflow, fall through
			}
		}
		BigDecimal bd = v1.toBigDecimal().subtract(v2.toBigDecimal());
		return fromBigDecimal(bd, precision, scale);
	}

	public static Decimal multiply(Decimal v1, Decimal v2, int precision, int scale) {
		BigDecimal bd = v1.toBigDecimal().multiply(v2.toBigDecimal());
		return fromBigDecimal(bd, precision, scale);
	}

	public static Decimal divide(Decimal v1, Decimal v2, int precision, int scale) {
		BigDecimal bd = v1.toBigDecimal().divide(v2.toBigDecimal(), MC_DIVIDE);
		return fromBigDecimal(bd, precision, scale);
	}

	public static Decimal mod(Decimal v1, Decimal v2, int precision, int scale) {
		BigDecimal bd = v1.toBigDecimal().remainder(v2.toBigDecimal(), MC_DIVIDE);
		return fromBigDecimal(bd, precision, scale);
	}

	/**
	 * Returns a {@code Decimal} whose value is the integer part
	 * of the quotient {@code (this / divisor)} rounded down.
	 *
	 * @param  value value by which this {@code Decimal} is to be divided.
	 * @param  divisor value by which this {@code Decimal} is to be divided.
	 * @return The integer part of {@code this / divisor}.
	 * @throws ArithmeticException if {@code divisor==0}
	 */
	public static Decimal divideToIntegralValue(Decimal value, Decimal divisor, int precision, int scale) {
		BigDecimal bd = value.toBigDecimal().divideToIntegralValue(divisor.toBigDecimal());
		return fromBigDecimal(bd, precision, scale);
	}

	// cast decimal to integral or floating data types, by SQL standard.
	// to cast to integer, rounding-DOWN is performed, and overflow will just return null.
	// to cast to floats, overflow will not happen, because precision<=38.

	public static long castToIntegral(Decimal dec) {
		BigDecimal bd = dec.toBigDecimal();
		// rounding down. This is consistent with float=>int,
		// and consistent with SQLServer, Spark.
		bd = bd.setScale(0, RoundingMode.DOWN);
		return bd.longValue();
	}

	public static long castToLong(Decimal dec) {
		return castToIntegral(dec);
	}

	public static int castToInt(Decimal dec) {
		return (int) castToIntegral(dec);
	}

	public static short castToShort(Decimal dec) {
		return (short) castToIntegral(dec);
	}

	public static byte castToByte(Decimal dec) {
		return (byte) castToIntegral(dec);
	}

	public static float castToFloat(Decimal dec) {
		return (float) dec.doubleValue();
	}

	public static double castToDouble(Decimal dec) {
		return dec.doubleValue();
	}

	public static Decimal castToDecimal(Decimal dec, int precision, int scale) {
		return fromBigDecimal(dec.toBigDecimal(), precision, scale);
	}

	public static boolean castToBoolean(Decimal dec) {
		return dec.toBigDecimal().compareTo(BigDecimal.ZERO) != 0;
	}

	public static long castToTimestamp(Decimal dec) {
		return (long) (dec.doubleValue() * 1000);
	}

	public static Decimal castFrom(Decimal dec, int precision, int scale) {
		return fromBigDecimal(dec.toBigDecimal(), precision, scale);
	}

	public static Decimal castFrom(String string, int precision, int scale) {
		return fromBigDecimal(new BigDecimal(string), precision, scale);
	}

	public static Decimal castFrom(double val, int p, int s) {
		return fromBigDecimal(BigDecimal.valueOf(val), p, s);
	}

	public static Decimal castFrom(long val, int p, int s) {
		return fromBigDecimal(BigDecimal.valueOf(val), p, s);
	}

	public static Decimal castFrom(boolean val, int p, int s) {
		return fromBigDecimal(BigDecimal.valueOf((val ? 1 : 0)), p, s);
	}

	/**
	 * SQL <code>SIGN</code> operator applied to BigDecimal values.
	 * preserve precision and scale.
	 */
	public static Decimal sign(Decimal b0) {
		if (b0.isCompact()) {
			return new Decimal(b0.precision, b0.scale, b0.signum() * POW10[b0.scale], null);
		} else {
			return fromBigDecimal(BigDecimal.valueOf(b0.signum()), b0.precision, b0.scale);
		}
	}

	public static int compare(Decimal b1, Decimal b2){
		return b1.compareTo(b2);
	}

	public static int compare(Decimal b1, long n2) {
		if (!b1.isCompact()) {
			return b1.decimalVal.compareTo(BigDecimal.valueOf(n2));
		}
		if (b1.scale == 0) {
			return Long.compare(b1.longVal, n2);
		}

		long i1 = b1.longVal / POW10[b1.scale];
		if (i1 == n2) {
			long l2 = n2 * POW10[b1.scale]; // won't overflow
			return Long.compare(b1.longVal, l2);
		} else {
			return i1 > n2 ? +1 : -1;
		}
	}

	public static int compare(Decimal b1, double n2) {
		return Double.compare(b1.doubleValue(), n2);
	}

	public static int compare(long n1, Decimal b2) {
		return -compare(b2, n1);
	}

	public static int compare(double n1, Decimal b2) {
		return -compare(b2, n1);
	}

	/**
	 * SQL <code>ROUND</code> operator applied to BigDecimal values.
	 */
	public static Decimal sround(Decimal b0, int r) {
		if (r >= b0.scale) {
			return b0;
		}

		BigDecimal b2 = b0.toBigDecimal().movePointRight(r)
				.setScale(0, RoundingMode.HALF_UP)
				.movePointLeft(r);
		int p = b0.precision;
		int s = b0.scale;
		if (r < 0) {
			return fromBigDecimal(b2, Math.min(38, 1 + p - s), 0);
		} else {  // 0 <= r < s
			return fromBigDecimal(b2, 1 + p - s + r, r);
		}
	}

	public static boolean is32BitDecimal(int precision) {
		return precision <= MAX_INT_DIGITS;
	}

	public static boolean is64BitDecimal(int precision) {
		return precision <= MAX_LONG_DIGITS && precision > MAX_INT_DIGITS;
	}

	public static boolean isByteArrayDecimal(int precision) {
		return precision > MAX_LONG_DIGITS;
	}

	static Decimal readDecimalFieldFromSegments(MemorySegment[] segments, int baseOffset,
			long offsetAndSize, int precision, int scale) {
		final int size = ((int) offsetAndSize);
		int subOffset = (int) (offsetAndSize >> 32);
		byte[] bytes = new byte[size];
		SegmentsUtil.copyToBytes(segments, baseOffset + subOffset, bytes, 0, size);
		return Decimal.fromUnscaledBytes(precision, scale, bytes);
	}
}
