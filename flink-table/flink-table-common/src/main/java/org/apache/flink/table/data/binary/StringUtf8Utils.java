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
import org.apache.flink.util.SwarUtils;
import org.apache.flink.util.Utf8Utils;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.table.data.binary.BinarySegmentUtils.allocateReuseBytes;
import static org.apache.flink.table.data.binary.BinarySegmentUtils.allocateReuseChars;

/** Utilities for String UTF-8. */
@Internal
public final class StringUtf8Utils {

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
        if (SwarUtils.isAscii(input, offset, byteLen)) {
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

    /**
     * Returns the index of the first byte in the range that breaks UTF-8 well-formedness, or {@code
     * -1} if the range is valid. See {@link Utf8Utils#firstInvalidUtf8ByteIndex}.
     */
    public static int firstInvalidUtf8ByteIndex(
            final byte[] bytes, final int offset, final int numBytes) {
        return Utf8Utils.firstInvalidUtf8ByteIndex(bytes, offset, numBytes);
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
