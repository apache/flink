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

package org.apache.flink.table.data;

import org.apache.flink.table.types.logical.DecimalType;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import static org.apache.flink.table.data.DecimalData.MAX_INT_DIGITS;
import static org.apache.flink.table.data.DecimalData.MAX_LONG_DIGITS;
import static org.apache.flink.table.data.DecimalData.POW10;
import static org.apache.flink.table.data.DecimalData.fromBigDecimal;

/**
 * Utilities for {@link DecimalData}.
 *
 * <p>Note: we have to put this class under the same package with {@link DecimalData} to make it
 * possible to access package-accessing member variables.
 */
public final class DecimalDataUtils {

    private static final MathContext MC_DIVIDE = new MathContext(38, RoundingMode.HALF_UP);

    public static final DecimalType DECIMAL_SYSTEM_DEFAULT =
            new DecimalType(DecimalType.MAX_PRECISION, 18);

    public static double doubleValue(DecimalData decimal) {
        if (decimal.isCompact()) {
            return ((double) decimal.longVal) / POW10[decimal.scale];
        } else {
            return decimal.decimalVal.doubleValue();
        }
    }

    /**
     * Returns the signum function of this decimal. (The return value is -1 if this decimal is
     * negative; 0 if this decimal is zero; and 1 if this decimal is positive.)
     *
     * @return the signum function of this decimal.
     */
    public static int signum(DecimalData decimal) {
        if (decimal.isCompact()) {
            return Long.signum(decimal.toUnscaledLong());
        } else {
            return decimal.toBigDecimal().signum();
        }
    }

    public static DecimalData negate(DecimalData decimal) {
        if (decimal.isCompact()) {
            return new DecimalData(decimal.precision, decimal.scale, -decimal.longVal, null);
        } else {
            return new DecimalData(
                    decimal.precision, decimal.scale, -1, decimal.decimalVal.negate());
        }
    }

    public static DecimalData abs(DecimalData decimal) {
        if (decimal.isCompact()) {
            if (decimal.longVal >= 0) {
                return decimal;
            } else {
                return new DecimalData(decimal.precision, decimal.scale, -decimal.longVal, null);
            }
        } else {
            if (decimal.decimalVal.signum() >= 0) {
                return decimal;
            } else {
                return new DecimalData(
                        decimal.precision, decimal.scale, -1, decimal.decimalVal.negate());
            }
        }
    }

    // floor()/ceil() preserve precision, but set scale to 0.
    // note that result may exceed the original precision.

    public static DecimalData floor(DecimalData decimal) {
        BigDecimal bd = decimal.toBigDecimal().setScale(0, RoundingMode.FLOOR);
        return fromBigDecimal(bd, bd.precision(), 0);
    }

    public static DecimalData ceil(DecimalData decimal) {
        BigDecimal bd = decimal.toBigDecimal().setScale(0, RoundingMode.CEILING);
        return fromBigDecimal(bd, bd.precision(), 0);
    }

    public static DecimalData add(DecimalData v1, DecimalData v2, int precision, int scale) {
        if (v1.isCompact() && v2.isCompact() && v1.scale == v2.scale) {
            assert scale == v1.scale; // no need to rescale
            try {
                long ls = Math.addExact(v1.longVal, v2.longVal); // checks overflow
                return new DecimalData(precision, scale, ls, null);
            } catch (ArithmeticException e) {
                // overflow, fall through
            }
        }
        BigDecimal bd = v1.toBigDecimal().add(v2.toBigDecimal());
        return fromBigDecimal(bd, precision, scale);
    }

    public static DecimalData subtract(DecimalData v1, DecimalData v2, int precision, int scale) {
        if (v1.isCompact() && v2.isCompact() && v1.scale == v2.scale) {
            assert scale == v1.scale; // no need to rescale
            try {
                long ls = Math.subtractExact(v1.longVal, v2.longVal); // checks overflow
                return new DecimalData(precision, scale, ls, null);
            } catch (ArithmeticException e) {
                // overflow, fall through
            }
        }
        BigDecimal bd = v1.toBigDecimal().subtract(v2.toBigDecimal());
        return fromBigDecimal(bd, precision, scale);
    }

    public static DecimalData multiply(DecimalData v1, DecimalData v2, int precision, int scale) {
        BigDecimal bd = v1.toBigDecimal().multiply(v2.toBigDecimal());
        return fromBigDecimal(bd, precision, scale);
    }

    public static DecimalData divide(DecimalData v1, DecimalData v2, int precision, int scale) {
        BigDecimal bd = v1.toBigDecimal().divide(v2.toBigDecimal(), MC_DIVIDE);
        return fromBigDecimal(bd, precision, scale);
    }

    public static DecimalData mod(DecimalData v1, DecimalData v2, int precision, int scale) {
        BigDecimal bd = v1.toBigDecimal().remainder(v2.toBigDecimal(), MC_DIVIDE);
        return fromBigDecimal(bd, precision, scale);
    }

    /**
     * Returns a {@code DecimalData} whose value is the integer part of the quotient {@code (this /
     * divisor)} rounded down.
     *
     * @param value value by which this {@code DecimalData} is to be divided.
     * @param divisor value by which this {@code DecimalData} is to be divided.
     * @return The integer part of {@code this / divisor}.
     * @throws ArithmeticException if {@code divisor==0}
     */
    public static DecimalData divideToIntegralValue(
            DecimalData value, DecimalData divisor, int precision, int scale) {
        BigDecimal bd = value.toBigDecimal().divideToIntegralValue(divisor.toBigDecimal());
        return fromBigDecimal(bd, precision, scale);
    }

    // cast decimal to integral or floating data types, by SQL standard.
    // to cast to integer, rounding-DOWN is performed, and overflow will just return null.
    // to cast to floats, overflow will not happen, because precision<=38.

    public static long castToIntegral(DecimalData dec) {
        BigDecimal bd = dec.toBigDecimal();
        // rounding down. This is consistent with float=>int,
        // and consistent with SQLServer, Spark.
        bd = bd.setScale(0, RoundingMode.DOWN);
        return bd.longValue();
    }

    public static long castToLong(DecimalData dec) {
        return castToIntegral(dec);
    }

    public static int castToInt(DecimalData dec) {
        return (int) castToIntegral(dec);
    }

    public static short castToShort(DecimalData dec) {
        return (short) castToIntegral(dec);
    }

    public static byte castToByte(DecimalData dec) {
        return (byte) castToIntegral(dec);
    }

    public static float castToFloat(DecimalData dec) {
        return (float) doubleValue(dec);
    }

    public static double castToDouble(DecimalData dec) {
        return doubleValue(dec);
    }

    public static DecimalData castToDecimal(DecimalData dec, int precision, int scale) {
        return fromBigDecimal(dec.toBigDecimal(), precision, scale);
    }

    public static boolean castToBoolean(DecimalData dec) {
        return dec.toBigDecimal().compareTo(BigDecimal.ZERO) != 0;
    }

    public static long castToTimestamp(DecimalData dec) {
        return (long) (doubleValue(dec) * 1000);
    }

    public static DecimalData castFrom(DecimalData dec, int precision, int scale) {
        return fromBigDecimal(dec.toBigDecimal(), precision, scale);
    }

    public static DecimalData castFrom(String string, int precision, int scale) {
        return fromBigDecimal(new BigDecimal(string), precision, scale);
    }

    public static DecimalData castFrom(double val, int p, int s) {
        return fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    public static DecimalData castFrom(long val, int p, int s) {
        return fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    public static DecimalData castFrom(boolean val, int p, int s) {
        return fromBigDecimal(BigDecimal.valueOf((val ? 1 : 0)), p, s);
    }

    /**
     * SQL <code>SIGN</code> operator applied to BigDecimal values. preserve precision and scale.
     */
    public static DecimalData sign(DecimalData b0) {
        if (b0.isCompact()) {
            return new DecimalData(b0.precision, b0.scale, signum(b0) * POW10[b0.scale], null);
        } else {
            return fromBigDecimal(BigDecimal.valueOf(signum(b0)), b0.precision, b0.scale);
        }
    }

    public static int compare(DecimalData b1, DecimalData b2) {
        return b1.compareTo(b2);
    }

    public static int compare(DecimalData b1, long n2) {
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

    public static int compare(DecimalData b1, double n2) {
        return Double.compare(doubleValue(b1), n2);
    }

    public static int compare(long n1, DecimalData b2) {
        return -compare(b2, n1);
    }

    public static int compare(double n1, DecimalData b2) {
        return -compare(b2, n1);
    }

    /** SQL <code>ROUND</code> operator applied to BigDecimal values. */
    public static DecimalData sround(DecimalData b0, int r) {
        if (r >= b0.scale) {
            return b0;
        }

        BigDecimal b2 =
                b0.toBigDecimal()
                        .movePointRight(r)
                        .setScale(0, RoundingMode.HALF_UP)
                        .movePointLeft(r);
        int p = b0.precision;
        int s = b0.scale;
        if (r < 0) {
            return fromBigDecimal(b2, Math.min(38, 1 + p - s), 0);
        } else { // 0 <= r < s
            return fromBigDecimal(b2, 1 + p - s + r, r);
        }
    }

    public static long power10(int n) {
        return POW10[n];
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
}
