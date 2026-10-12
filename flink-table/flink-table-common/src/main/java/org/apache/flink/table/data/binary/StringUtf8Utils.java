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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.data.StringData;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static org.apache.flink.table.data.binary.BinarySegmentUtils.allocateReuseBytes;
import static org.apache.flink.table.data.binary.BinarySegmentUtils.allocateReuseChars;

/** Utilities for String UTF-8. */
@Internal
public final class StringUtf8Utils {

    /** Reads 8 bytes at a time from a {@code byte[]} for the SWAR ASCII scan. */
    private static final VarHandle LONG_VIEW =
            MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    /** High bit of each byte in a 64-bit word; a set bit marks a non-ASCII byte. */
    private static final long ASCII_MASK = 0x8080808080808080L;

    private StringUtf8Utils() {
        // do not instantiate
    }

    public static byte[] encodeUTF8(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static String decodeUTF8(byte[] input, int offset, int byteLen) {
        // Most real text is ASCII: route it to the JDK's compact-string path, which is a large win
        // for longer strings. Anything with a multibyte sequence goes through the hand-rolled
        // decoder; it beats the CharsetDecoder path for non-ASCII input.
        if (isAscii(input, offset, byteLen)) {
            // Pure ASCII: bytes map 1:1 to chars; ISO-8859-1 is an intrinsic LATIN1 copy.
            return new String(input, offset, byteLen, StandardCharsets.ISO_8859_1);
        }
        char[] chars = allocateReuseChars(byteLen);
        int len = decodeUTF8Strict(input, offset, byteLen, chars);
        if (len < 0) {
            // Malformed input; map to U+FFFD via the JDK decoder (matches the previous fallback).
            return new String(input, offset, byteLen, StandardCharsets.UTF_8);
        }
        return new String(chars, 0, len);
    }

    /**
     * SWAR ASCII test: reads 8 bytes per step and checks the high bit of each via {@link
     * #ASCII_MASK}, so a fully ASCII range is scanned ~8x faster than byte-by-byte and non-ASCII
     * input bails at the first eight-byte block that contains a set high bit.
     */
    private static boolean isAscii(byte[] bytes, int offset, int len) {
        int i = offset;
        final int end = offset + len;
        final int swarEnd = offset + (len & ~7);
        while (i < swarEnd) {
            if (((long) LONG_VIEW.get(bytes, i) & ASCII_MASK) != 0) {
                return false;
            }
            i += 8;
        }
        while (i < end) {
            if (bytes[i] < 0) {
                return false;
            }
            i++;
        }
        return true;
    }

    public static int decodeUTF8Strict(byte[] sa, int sp, int len, char[] da) {
        final int sl = sp + len;
        int dp = 0;
        int dlASCII = Math.min(len, da.length);

        // ASCII only optimized loop
        while (dp < dlASCII && sa[sp] >= 0) {
            da[dp++] = (char) sa[sp++];
        }

        while (sp < sl) {
            int b1 = sa[sp++];
            if (b1 >= 0) {
                // 1 byte, 7 bits: 0xxxxxxx
                da[dp++] = (char) b1;
            } else if ((b1 >> 5) == -2 && (b1 & 0x1e) != 0) {
                // 2 bytes, 11 bits: 110xxxxx 10xxxxxx
                if (sp < sl) {
                    int b2 = sa[sp++];
                    if ((b2 & 0xc0) != 0x80) { // isNotContinuation(b2)
                        return -1;
                    } else {
                        da[dp++] = (char) (((b1 << 6) ^ b2) ^ (((byte) 0xC0 << 6) ^ ((byte) 0x80)));
                    }
                    continue;
                }
                return -1;
            } else if ((b1 >> 4) == -2) {
                // 3 bytes, 16 bits: 1110xxxx 10xxxxxx 10xxxxxx
                if (sp + 1 < sl) {
                    int b2 = sa[sp++];
                    int b3 = sa[sp++];
                    if ((b1 == (byte) 0xe0 && (b2 & 0xe0) == 0x80)
                            || (b2 & 0xc0) != 0x80
                            || (b3 & 0xc0) != 0x80) { // isMalformed3(b1, b2, b3)
                        return -1;
                    } else {
                        char c =
                                (char)
                                        ((b1 << 12)
                                                ^ (b2 << 6)
                                                ^ (b3
                                                        ^ (((byte) 0xE0 << 12)
                                                                ^ ((byte) 0x80 << 6)
                                                                ^ ((byte) 0x80))));
                        if (Character.isSurrogate(c)) {
                            return -1;
                        } else {
                            da[dp++] = c;
                        }
                    }
                    continue;
                }
                return -1;
            } else if ((b1 >> 3) == -2) {
                // 4 bytes, 21 bits: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                if (sp + 2 < sl) {
                    int b2 = sa[sp++];
                    int b3 = sa[sp++];
                    int b4 = sa[sp++];
                    int uc =
                            ((b1 << 18)
                                    ^ (b2 << 12)
                                    ^ (b3 << 6)
                                    ^ (b4
                                            ^ (((byte) 0xF0 << 18)
                                                    ^ ((byte) 0x80 << 12)
                                                    ^ ((byte) 0x80 << 6)
                                                    ^ ((byte) 0x80))));
                    // isMalformed4 and shortest form check
                    if (((b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80 || (b4 & 0xc0) != 0x80)
                            || !Character.isSupplementaryCodePoint(uc)) {
                        return -1;
                    } else {
                        da[dp++] = Character.highSurrogate(uc);
                        da[dp++] = Character.lowSurrogate(uc);
                    }
                    continue;
                }
                return -1;
            } else {
                return -1;
            }
        }
        return dp;
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

    /**
     * Returns the UTF-8 bytes of the given string. Every malformed sequence is replaced by the
     * U+FFFD replacement character, the same as {@link StringData#toString()} decodes it. The
     * returned array may be shared with the string, so it must not be modified.
     */
    public static byte[] toValidUtf8Bytes(final StringData string) {
        final BinaryStringData binaryString = (BinaryStringData) string;
        if (binaryString.getBinarySection() == null) {
            // A string that only exists as a Java object, such as a literal or a function result,
            // is valid UTF-8 once encoded. Encoding it here skips the binary form and the check.
            return encodeUTF8(binaryString.getJavaObject());
        }
        final byte[] bytes = binaryString.toBytes();
        if (firstInvalidUtf8ByteIndex(bytes, 0, bytes.length) < 0) {
            return bytes;
        }
        return encodeUTF8(defaultDecodeUTF8(bytes, 0, bytes.length));
    }

    public static String decodeUTF8(MemorySegment input, int offset, int byteLen) {
        char[] chars = allocateReuseChars(byteLen);
        int len = decodeUTF8Strict(input, offset, byteLen, chars);
        if (len < 0) {
            byte[] bytes = allocateReuseBytes(byteLen);
            input.get(offset, bytes, 0, byteLen);
            return defaultDecodeUTF8(bytes, 0, byteLen);
        }
        return new String(chars, 0, len);
    }

    public static String defaultDecodeUTF8(byte[] bytes, int offset, int len) {
        return new String(bytes, offset, len, StandardCharsets.UTF_8);
    }

    public static int decodeUTF8Strict(MemorySegment segment, int sp, int len, char[] da) {
        final int sl = sp + len;
        int dp = 0;
        int dlASCII = Math.min(len, da.length);

        // ASCII only optimized loop
        while (dp < dlASCII && segment.get(sp) >= 0) {
            da[dp++] = (char) segment.get(sp++);
        }

        while (sp < sl) {
            int b1 = segment.get(sp++);
            if (b1 >= 0) {
                // 1 byte, 7 bits: 0xxxxxxx
                da[dp++] = (char) b1;
            } else if ((b1 >> 5) == -2 && (b1 & 0x1e) != 0) {
                // 2 bytes, 11 bits: 110xxxxx 10xxxxxx
                if (sp < sl) {
                    int b2 = segment.get(sp++);
                    if ((b2 & 0xc0) != 0x80) { // isNotContinuation(b2)
                        return -1;
                    } else {
                        da[dp++] = (char) (((b1 << 6) ^ b2) ^ (((byte) 0xC0 << 6) ^ ((byte) 0x80)));
                    }
                    continue;
                }
                return -1;
            } else if ((b1 >> 4) == -2) {
                // 3 bytes, 16 bits: 1110xxxx 10xxxxxx 10xxxxxx
                if (sp + 1 < sl) {
                    int b2 = segment.get(sp++);
                    int b3 = segment.get(sp++);
                    if ((b1 == (byte) 0xe0 && (b2 & 0xe0) == 0x80)
                            || (b2 & 0xc0) != 0x80
                            || (b3 & 0xc0) != 0x80) { // isMalformed3(b1, b2, b3)
                        return -1;
                    } else {
                        char c =
                                (char)
                                        ((b1 << 12)
                                                ^ (b2 << 6)
                                                ^ (b3
                                                        ^ (((byte) 0xE0 << 12)
                                                                ^ ((byte) 0x80 << 6)
                                                                ^ ((byte) 0x80))));
                        if (Character.isSurrogate(c)) {
                            return -1;
                        } else {
                            da[dp++] = c;
                        }
                    }
                    continue;
                }
                return -1;
            } else if ((b1 >> 3) == -2) {
                // 4 bytes, 21 bits: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
                if (sp + 2 < sl) {
                    int b2 = segment.get(sp++);
                    int b3 = segment.get(sp++);
                    int b4 = segment.get(sp++);
                    int uc =
                            ((b1 << 18)
                                    ^ (b2 << 12)
                                    ^ (b3 << 6)
                                    ^ (b4
                                            ^ (((byte) 0xF0 << 18)
                                                    ^ ((byte) 0x80 << 12)
                                                    ^ ((byte) 0x80 << 6)
                                                    ^ ((byte) 0x80))));
                    // isMalformed4 and shortest form check
                    if (((b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80 || (b4 & 0xc0) != 0x80)
                            || !Character.isSupplementaryCodePoint(uc)) {
                        return -1;
                    } else {
                        da[dp++] = Character.highSurrogate(uc);
                        da[dp++] = Character.lowSurrogate(uc);
                    }
                    continue;
                }
                return -1;
            } else {
                return -1;
            }
        }
        return dp;
    }
}
