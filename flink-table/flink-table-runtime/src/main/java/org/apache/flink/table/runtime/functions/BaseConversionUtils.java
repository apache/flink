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

package org.apache.flink.table.runtime.functions;

import java.util.Arrays;

/** This is inspired by Hive: org.apache.hadoop.hive.ql.udf.UDFConv.java. */
public class BaseConversionUtils {

    /**
     * The output string has a max length of one char per bit in the 64-bit `Long` intermediate
     * representation plus one char for the '-' sign. This happens in practice when converting
     * `Long.MinValue` with `toBase` equal to -2.
     */
    private static final int MAX_OUTPUT_LENGTH = Long.SIZE + 1;

    /**
     * Divide x by m as if x is an unsigned 64-bit integer. Examples: unsignedLongDiv(-1, 2) ==
     * Long.MAX_VALUE unsignedLongDiv(6, 3) == 2 unsignedLongDiv(0, 5) == 0
     *
     * @param x is treated as unsigned
     * @param m is treated as signed
     */
    private static long unsignedLongDiv(long x, int m) {
        if (x >= 0) {
            return x / m;
        }

        // Let uval be the value of the unsigned long with the same bits as x
        // Two's complement => x = uval - 2 * MAX - 2
        // => uval = x + 2 * MAX + 2
        // Now, use the fact: (a + b) / c = a / c + b / c + (a % c + b % c) / c

        // remainder < 0 is invalid for unsigned long. Therefore, div result should be decremented
        // by 1 if remainder % m < 0
        long remainder = (x % m + 2 * (Long.MAX_VALUE % m) + 2 % m);
        if (remainder < 0) {
            remainder -= m - 1;
        }

        return x / m + 2 * (Long.MAX_VALUE / m) + 2 / m + remainder / m;
    }

    /**
     * Decode val into value[].
     *
     * @param val is treated as an unsigned 64-bit integer
     * @param radix must be between MIN_RADIX and MAX_RADIX
     */
    private static void decode(byte[] value, long val, int radix) {
        Arrays.fill(value, (byte) 0);
        for (int i = value.length - 1; val != 0; i--) {
            long q = unsignedLongDiv(val, radix);
            value[i] = (byte) (val - q * radix);
            val = q;
        }
    }

    /**
     * Convert value[] into a long. On overflow, return -1 (as MySQL does).
     *
     * @param radix must be between MIN_RADIX and MAX_RADIX
     * @param fromPos is the first element that should be considered
     * @return the result should be treated as an unsigned 64-bit integer.
     */
    private static long encode(byte[] value, int radix, int fromPos) {
        long val = 0;

        // bound will always be positive since radix >= 2.
        // -1 is reserved to indicate overflows.
        // possible overflow once.
        long bound = unsignedLongDiv(-1 - radix, radix);

        for (int i = fromPos; i < value.length && value[i] >= 0; i++) {
            // val < 0 means its bit presentation starts with 1, val * radix will cause overflow
            if (val < 0) {
                return -1;
            }

            // bound is not accurate enough, our target is checking whether val * radix + value(i)
            // can cause overflow or not.
            // Just like bound, (-1 - value(i)) / radix will be positive, and we can easily check
            // overflow by checking (-1 - value(i)) / radix < val or not.
            if (val >= bound && unsignedLongDiv(-1 - value[i], radix) < val) {
                return -1;
            }

            val = val * radix + value[i];
        }
        return val;
    }

    /**
     * Convert the bytes in value[] to the corresponding chars.
     *
     * @param radix must be between MIN_RADIX and MAX_RADIX
     * @param fromPos is the first nonzero element
     */
    private static void byte2char(byte[] value, int radix, int fromPos) {
        for (int i = fromPos; i < value.length; i++) {
            value[i] = (byte) Character.toUpperCase(Character.forDigit(value[i], radix));
        }
    }

    /**
     * Convert the chars in value[] to the corresponding integers. If invalid character is found,
     * convert it to -1 and ignore the suffix starting there.
     *
     * @param radix must be between MIN_RADIX and MAX_RADIX
     * @param fromPos is the first nonzero element
     */
    private static void char2byte(byte[] value, int radix, int fromPos) {
        for (int i = fromPos; i < value.length; i++) {
            value[i] = (byte) Character.digit(value[i], radix);
            if (value[i] == -1) {
                return;
            }
        }
    }

    /**
     * Convert numbers between different number bases. If {@code toBase} is negative, {@code num} is
     * interpreted as a signed number, otherwise it is treated as an unsigned number. The result is
     * consistent with this rule.
     */
    public static String conv(byte[] num, long fromBase, long toBase) {
        if (num == null || num.length < 1) {
            return null;
        }

        if (fromBase < Character.MIN_RADIX
                || fromBase > Character.MAX_RADIX
                || toBase == Long.MIN_VALUE
                || Math.abs(toBase) < Character.MIN_RADIX
                || Math.abs(toBase) > Character.MAX_RADIX) {
            return null;
        }

        int fromBaseInt = (int) fromBase;
        int toBaseInt = (int) toBase;

        boolean negative = (num[0] == '-');
        int first = negative ? 1 : 0;

        byte[] value = new byte[Math.max(num.length, MAX_OUTPUT_LENGTH)];
        System.arraycopy(num, first, value, value.length - num.length + first, num.length - first);
        char2byte(value, fromBaseInt, value.length - num.length + first);

        // do the conversion by going through a 64-bit integer
        long val = encode(value, fromBaseInt, value.length - num.length + first);
        if (val == -1) {
            return null;
        }

        // use negative num to represent a num bigger than LONG_MAX
        if (negative && toBaseInt > 0) {
            if (val < 0) {
                // double negative is invalid
                val = -1;
            } else {
                // get actual num
                val = -val;
            }
        }

        if (toBaseInt < 0 && val < 0) {
            val = -val;
            negative = true;
        }

        decode(value, val, Math.abs(toBaseInt));

        // find the first non-zero digit or the last digits if all are zero
        for (first = 0; first < value.length - 1 && value[first] == 0; first++) {}

        byte2char(value, Math.abs(toBaseInt), first);

        if (negative && toBaseInt < 0) {
            value[--first] = '-';
        }

        return new String(value, first, value.length - first);
    }
}
