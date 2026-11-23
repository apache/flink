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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

/**
 * Scans of byte ranges that test 8 bytes at once as one {@code long} (SIMD within a register), so
 * they don't depend on the JIT vectorizing a loop.
 */
@Internal
public final class SwarUtils {

    // Byte order doesn't matter for checking whether any byte of a word matches.
    private static final VarHandle LONG_VIEW =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private static final long ONES = 0x0101010101010101L;
    private static final long HIGH_BITS = 0x8080808080808080L;

    private SwarUtils() {}

    /** Whether {@code bytes[offset, offset + len)} contains only ASCII bytes. */
    public static boolean isAscii(byte[] bytes, int offset, int len) {
        int i = offset;
        final int end = offset + len;
        for (; end - i >= Long.BYTES; i += Long.BYTES) {
            if ((readWord(bytes, i) & HIGH_BITS) != 0) {
                return false;
            }
        }
        for (; i < end; i++) {
            if (bytes[i] < 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether {@code bytes[offset, offset + len)} contains {@code b1}, {@code b2} or a byte below
     * {@code lessThan}, comparing bytes unsigned.
     *
     * @param lessThan at most {@code 0x80}
     */
    public static boolean containsAnyOrLessThan(
            byte[] bytes, int offset, int len, byte b1, byte b2, int lessThan) {
        return containsLessThanOrAny(bytes, offset, len, true, b1, b2, lessThan);
    }

    /**
     * Whether {@code bytes[offset, offset + len)} contains a byte below {@code lessThan}, comparing
     * bytes unsigned.
     *
     * @param lessThan at most {@code 0x80}
     */
    public static boolean containsLessThan(byte[] bytes, int offset, int len, int lessThan) {
        return containsLessThanOrAny(bytes, offset, len, false, (byte) 0, (byte) 0, lessThan);
    }

    private static boolean containsLessThanOrAny(
            byte[] bytes, int offset, int len, boolean matchBytes, byte b1, byte b2, int lessThan) {
        int i = offset;
        final int end = offset + len;
        for (; end - i >= Long.BYTES; i += Long.BYTES) {
            final long word = readWord(bytes, i);
            long found = hasByteLessThan(word, lessThan);
            if (matchBytes) {
                found |= hasByte(word, b1) | hasByte(word, b2);
            }
            if (found != 0) {
                return true;
            }
        }
        for (; i < end; i++) {
            final byte b = bytes[i];
            if ((b & 0xFF) < lessThan || (matchBytes && (b == b1 || b == b2))) {
                return true;
            }
        }
        return false;
    }

    private static long readWord(byte[] bytes, int index) {
        return (long) LONG_VIEW.get(bytes, index);
    }

    // Non-zero if the word contains b: XOR turns a matching byte into zero, then finds a zero byte.
    private static long hasByte(long word, byte b) {
        final long x = word ^ (ONES * (b & 0xFF));
        return (x - ONES) & ~x & HIGH_BITS;
    }

    // Non-zero if the word contains a byte below n; "& ~word" ignores bytes of 0x80 and above.
    private static long hasByteLessThan(long word, int n) {
        return (word - ONES * n) & ~word & HIGH_BITS;
    }
}
