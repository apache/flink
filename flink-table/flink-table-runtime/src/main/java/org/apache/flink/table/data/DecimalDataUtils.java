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

import javax.annotation.Nullable;

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
            return decimal.toBigDecimal().doubleValue();
        }
    }

    /**
     * Returns the signum function of this decimal. (The return value is -1 if this decimal is
     * negative; 0 if this decimal is zero; and 1 if this decimal is positive.)
     *
     * @return the signum function of this decimal.
     */
    public static int signum(DecimalData decimal) {
        if (decimal.longBacked) {
            return Long.signum(decimal.longVal);
        } else {
            return decimal.toBigDecimal().signum();
        }
    }

    public static DecimalData negate(DecimalData decimal) {
        if (decimal.longBacked && decimal.longVal != Long.MIN_VALUE) {
            return new DecimalData(decimal.precision, decimal.scale, -decimal.longVal, null);
        } else {
            return new DecimalData(
                    decimal.precision, decimal.scale, -1, decimal.toBigDecimal().negate());
        }
    }

    public static DecimalData abs(DecimalData decimal) {
        if (decimal.longBacked && decimal.longVal != Long.MIN_VALUE) {
            if (decimal.longVal >= 0) {
                return decimal;
            } else {
                return new DecimalData(decimal.precision, decimal.scale, -decimal.longVal, null);
            }
        } else {
            final BigDecimal bd = decimal.toBigDecimal();
            if (bd.signum() >= 0) {
                return decimal;
            } else {
                return new DecimalData(decimal.precision, decimal.scale, -1, bd.negate());
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
        if (canAddOrSubtractAsLong(v1, v2, precision, scale)) {
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
        if (canAddOrSubtractAsLong(v1, v2, precision, scale)) {
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

    private static boolean canAddOrSubtractAsLong(
            DecimalData v1, DecimalData v2, int precision, int scale) {
        return (DecimalData.isCompact(precision)
                        ? v1.isCompact() && v2.isCompact()
                        : v1.longBacked && v2.longBacked)
                && v1.scale == scale
                && v2.scale == scale;
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

    public static DecimalData castToDecimal(DecimalData dec, int precision, int scale) {
        if (dec.longBacked) {
            if (DecimalData.isCompact(precision)) {
                if (dec.scale < POW10.length) {
                    return DecimalData.fromCompactRescale(dec.longVal, dec.scale, precision, scale);
                }
            } else {
                final DecimalData scaled =
                        rescaleUpAsLong(dec.longVal, dec.scale, precision, scale);
                if (scaled != null) {
                    return scaled;
                }
            }
        }
        return fromBigDecimal(dec.toBigDecimal(), precision, scale);
    }

    // scale must not decrease; null if the result overflows a long
    private static @Nullable DecimalData rescaleUpAsLong(
            long unscaled, int fromScale, int precision, int scale) {
        final int scaleDiff = scale - fromScale;
        if (scaleDiff < 0 || scaleDiff >= POW10.length) {
            return null;
        }
        try {
            return new DecimalData(
                    precision, scale, Math.multiplyExact(unscaled, POW10[scaleDiff]), null);
        } catch (ArithmeticException e) {
            return null;
        }
    }

    public static DecimalData castFrom(DecimalData dec, int precision, int scale) {
        return castToDecimal(dec, precision, scale);
    }

    public static DecimalData castFrom(String string, int precision, int scale) {
        return fromBigDecimal(new BigDecimal(string), precision, scale);
    }

    public static DecimalData castFrom(double val, int p, int s) {
        return fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    public static DecimalData castFrom(long val, int p, int s) {
        if (DecimalData.isCompact(p)) {
            return DecimalData.fromCompactRescale(val, 0, p, s);
        }
        final DecimalData scaled = rescaleUpAsLong(val, 0, p, s);
        if (scaled != null) {
            return scaled;
        }
        return fromBigDecimal(BigDecimal.valueOf(val), p, s);
    }

    public static boolean castToBoolean(DecimalData dec) {
        return dec.toBigDecimal().compareTo(BigDecimal.ZERO) != 0;
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
        if (!b1.longBacked || b1.scale >= POW10.length) {
            return b1.toBigDecimal().compareTo(BigDecimal.valueOf(n2));
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
