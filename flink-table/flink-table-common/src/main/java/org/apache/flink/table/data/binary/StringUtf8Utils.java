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

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;

import java.nio.charset.StandardCharsets;

/** Utilities for String UTF-8. */
@Internal
public final class StringUtf8Utils {

    private StringUtf8Utils() {
        // do not instantiate
    }

    /** This method must have the same result with JDK's String.getBytes. */
    public static byte[] encodeUTF8(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static String decodeUTF8(byte[] input, int offset, int byteLen) {
        // JDK-intrinsified decode; maps malformed input to U+FFFD (matches the previous fallback).
        return new String(input, offset, byteLen, StandardCharsets.UTF_8);
    }

    // Bit-pattern predicates for UTF-8 byte categorization. The JIT inlines these so they cost
    // nothing at runtime, but they make {@link #firstInvalidUtf8ByteIndex} read like prose.
    private static boolean isAsciiByte(int b) {
        return b >= 0;
    }

    private static boolean is2ByteLead(int b) {
        // 110xxxxx; (b & 0x1e) != 0 rejects the overlong leads 0xC0 and 0xC1
        return (b >> 5) == -2 && (b & 0x1e) != 0;
    }

    private static boolean is3ByteLead(int b) {
        return (b >> 4) == -2; // 1110xxxx
    }

    private static boolean is4ByteLead(int b) {
        return (b >> 3) == -2; // 11110xxx
    }

    private static boolean isContinuation(int b) {
        return (b & 0xc0) == 0x80; // 10xxxxxx
    }

    private static boolean isOverlong3(int b1, int b2) {
        // 0xE0 followed by 0x80-0x9F encodes a code point already representable in 2 bytes
        return b1 == (byte) 0xe0 && (b2 & 0xe0) == 0x80;
    }

    private static char decode3ByteSequence(int b1, int b2, int b3) {
        return (char)
                ((b1 << 12)
                        ^ (b2 << 6)
                        ^ (b3 ^ (((byte) 0xE0 << 12) ^ ((byte) 0x80 << 6) ^ ((byte) 0x80))));
    }

    private static int decode4ByteSequence(int b1, int b2, int b3, int b4) {
        return (b1 << 18)
                ^ (b2 << 12)
                ^ (b3 << 6)
                ^ (b4
                        ^ (((byte) 0xF0 << 18)
                                ^ ((byte) 0x80 << 12)
                                ^ ((byte) 0x80 << 6)
                                ^ ((byte) 0x80)));
    }

    /**
     * Returns the absolute index (into {@code bytes}) of the first byte that breaks UTF-8
     * well-formedness, or {@code -1} if the range is valid. For a truncated trailing sequence the
     * returned index is {@code offset + numBytes} (one past the last byte) since the failure is the
     * absence of an expected continuation byte. Same byte-level checks as {@link
     * #decodeUTF8Strict(byte[], int, int, char[])} but without the char-buffer write side effect.
     *
     * <p>This is a hot per-record path; it trusts its inputs and does not validate them. A non-null
     * {@code bytes} with non-negative {@code offset} / {@code numBytes} that fits within the array
     * is required; misuse may throw {@link NullPointerException} or {@link
     * ArrayIndexOutOfBoundsException}.
     */
    public static int firstInvalidUtf8ByteIndex(
            final byte[] bytes, final int offset, final int numBytes) {
        int sp = offset;
        final int sl = sp + numBytes;

        // ASCII fast-path
        while (sp < sl && isAsciiByte(bytes[sp])) {
            sp++;
        }

        while (sp < sl) {
            final int start = sp;
            final int b1 = bytes[sp++];

            if (isAsciiByte(b1)) {
                continue;
            }

            if (is2ByteLead(b1)) {
                if (sp >= sl) {
                    return sl;
                }
                if (!isContinuation(bytes[sp++])) {
                    return start;
                }
                continue;
            }

            if (is3ByteLead(b1)) {
                if (sp + 1 >= sl) {
                    return sl;
                }
                final int b2 = bytes[sp++];
                final int b3 = bytes[sp++];
                if (isOverlong3(b1, b2) || !isContinuation(b2) || !isContinuation(b3)) {
                    return start;
                }
                if (Character.isSurrogate(decode3ByteSequence(b1, b2, b3))) {
                    return start;
                }
                continue;
            }

            if (is4ByteLead(b1)) {
                if (sp + 2 >= sl) {
                    return sl;
                }
                final int b2 = bytes[sp++];
                final int b3 = bytes[sp++];
                final int b4 = bytes[sp++];
                if (!isContinuation(b2) || !isContinuation(b3) || !isContinuation(b4)) {
                    return start;
                }
                // Shortest-form check catches both overlong 4-byte forms and code points
                // above U+10FFFF (anything not in the supplementary plane is invalid here).
                if (!Character.isSupplementaryCodePoint(decode4ByteSequence(b1, b2, b3, b4))) {
                    return start;
                }
                continue;
            }

            // Continuation byte without a lead, or a 5+-byte lead (RFC 3629 forbids).
            return start;
        }
        return -1;
    }
}
