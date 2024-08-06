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

/**
 * This is ported from Hive: org.apache.hadoop.hive.ql.udf.UDFConv.java. Just modify conv function
 * return type to prevent codec overhead.
 */
public class BaseConversionUtils {
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
        // Two's complement => x = uval - 2*MAX - 2
        // => uval = x + 2*MAX + 2
        // Now, use the fact: (a+b)/c = a/c + b/c + (a%c+b%c)/c
        return x / m
                + 2 * (Long.MAX_VALUE / m)
                + 2 / m
                + (x % m + 2 * (Long.MAX_VALUE % m) + 2 % m) / m;
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
     * Convert value[] into a long. On overflow, return -1 (as mySQL does). If a negative digit is
     * found, ignore the suffix starting there.
     *
     * @param radix must be between MIN_RADIX and MAX_RADIX
     * @param fromPos is the first element that should be conisdered
     * @return the result should be treated as an unsigned 64-bit integer.
     */
    private static long encode(byte[] value, int radix, int fromPos) {
        long val = 0;
        long bound = unsignedLongDiv(-1 - radix, radix); // Possible overflow once
        // val
        // exceeds this value
        for (int i = fromPos; i < value.length && value[i] >= 0; i++) {
            if (val >= bound) {
                // Check for overflow
                if (unsignedLongDiv(-1 - value[i], radix) < val) {
                    return -1;
                }
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
     * Convert the chars in value[] to the corresponding integers. Convert invalid characters to -1.
     *
     * @param radix must be between MIN_RADIX and MAX_RADIX
     * @param fromPos is the first nonzero element
     */
    private static boolean char2byte(byte[] value, int radix, int fromPos) {
        for (int i = fromPos; i < value.length; i++) {
            value[i] = (byte) Character.digit(value[i], radix);
            if (value[i] == -1) {
                return false;
            }
        }
        return true;
    }

    /**
     * Convert numbers between different number bases. If toBase&gt;0 the result is unsigned,
     * otherwise it is signed.
     */
    public static byte[] conv(byte[] n, int fromBase, int toBase) {
        byte[] value = new byte[64];
        if (n == null || n.length < 1) {
            return null;
        }

        if (fromBase < Character.MIN_RADIX
                || fromBase > Character.MAX_RADIX
                || Math.abs(toBase) < Character.MIN_RADIX
                || Math.abs(toBase) > Character.MAX_RADIX) {
            return null;
        }

        boolean negative = (n[0] == '-');
        int first = 0;
        if (negative) {
            first = 1;
        }

        // Copy the digits in the right side of the array
        for (int i = 1; i <= n.length - first; i++) {
            value[value.length - i] = n[n.length - i];
        }
        if (!char2byte(value, fromBase, value.length - n.length + first)) {
            return null;
        }

        // Do the conversion by going through a 64 bit integer
        long val = encode(value, fromBase, value.length - n.length + first);
        if (negative && toBase > 0) {
            if (val < 0) {
                val = -1;
            } else {
                val = -val;
            }
        }
        if (toBase < 0 && val < 0) {
            val = -val;
            negative = true;
        }
        decode(value, val, Math.abs(toBase));

        // Find the first non-zero digit or the last digits if all are zero.
        for (first = 0; first < value.length - 1 && value[first] == 0; first++) {}

        byte2char(value, Math.abs(toBase), first);

        if (negative && toBase < 0) {
            value[--first] = '-';
        }

        return Arrays.copyOfRange(value, first, value.length);
    }
}
