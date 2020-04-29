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

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.apache.flink.table.data.binary.BinarySegmentUtils.allocateReuseBytes;
import static org.apache.flink.table.data.binary.BinarySegmentUtils.allocateReuseChars;

/**
 * Utilities for String UTF-8.
 */
@Internal
final class StringUtf8Utils {

	private static final int MAX_BYTES_PER_CHAR = 3;

	private StringUtf8Utils() {
		// do not instantiate
	}

	/**
	 * This method must have the same result with JDK's String.getBytes.
	 */
	public static byte[] encodeUTF8(String str) {
		byte[] bytes = allocateReuseBytes(str.length() * MAX_BYTES_PER_CHAR);
		int len = encodeUTF8(str, bytes);
		return Arrays.copyOf(bytes, len);
	}

	public static int encodeUTF8(String str, byte[] bytes) {
		int offset = 0;
		int len = str.length();
		int sl = offset + len;
		int dp = 0;
		int dlASCII = dp + Math.min(len, bytes.length);

		// ASCII only optimized loop
		while (dp < dlASCII && str.charAt(offset) < '\u0080') {
			bytes[dp++] = (byte) str.charAt(offset++);
		}

		while (offset < sl) {
			char c = str.charAt(offset++);
			if (c < 0x80) {
				// Have at most seven bits
				bytes[dp++] = (byte) c;
			} else if (c < 0x800) {
				// 2 bytes, 11 bits
				bytes[dp++] = (byte) (0xc0 | (c >> 6));
				bytes[dp++] = (byte) (0x80 | (c & 0x3f));
			} else if (Character.isSurrogate(c)) {
				final int uc;
				int ip = offset - 1;
				if (Character.isHighSurrogate(c)) {
					if (sl - ip < 2) {
						uc = -1;
					} else {
						char d = str.charAt(ip + 1);
						if (Character.isLowSurrogate(d)) {
							uc = Character.toCodePoint(c, d);
						} else {
							// for some illegal character
							// the jdk will ignore the origin character and cast it to '?'
							// this acts the same with jdk
							return defaultEncodeUTF8(str, bytes);
						}
					}
				} else {
					if (Character.isLowSurrogate(c)) {
						// for some illegal character
						// the jdk will ignore the origin character and cast it to '?'
						// this acts the same with jdk
						return defaultEncodeUTF8(str, bytes);
					} else {
						uc = c;
					}
				}

				if (uc < 0) {
					bytes[dp++] = (byte) '?';
				} else {
					bytes[dp++] = (byte) (0xf0 | ((uc >> 18)));
					bytes[dp++] = (byte) (0x80 | ((uc >> 12) & 0x3f));
					bytes[dp++] = (byte) (0x80 | ((uc >> 6) & 0x3f));
					bytes[dp++] = (byte) (0x80 | (uc & 0x3f));
					offset++; // 2 chars
				}
			} else {
				// 3 bytes, 16 bits
				bytes[dp++] = (byte) (0xe0 | ((c >> 12)));
				bytes[dp++] = (byte) (0x80 | ((c >> 6) & 0x3f));
				bytes[dp++] = (byte) (0x80 | (c & 0x3f));
			}
		}
		return dp;
	}

	public static int defaultEncodeUTF8(String str, byte[] bytes) {
		try {
			byte[] buffer = str.getBytes("UTF-8");
			System.arraycopy(buffer, 0, bytes, 0, buffer.length);
			return buffer.length;
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("encodeUTF8 error", e);
		}
	}

	public static String decodeUTF8(byte[] input, int offset, int byteLen) {
		char[] chars = allocateReuseChars(byteLen);
		int len = decodeUTF8Strict(input, offset, byteLen, chars);
		if (len < 0) {
			return defaultDecodeUTF8(input, offset, byteLen);
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
						char c = (char) ((b1 << 12) ^ (b2 << 6) ^ (b3 ^
							(((byte) 0xE0 << 12) ^ ((byte) 0x80 << 6) ^ ((byte) 0x80))));
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
					int uc = ((b1 << 18) ^
						(b2 << 12) ^
						(b3 << 6) ^
						(b4 ^ (((byte) 0xF0 << 18) ^ ((byte) 0x80 << 12) ^
							((byte) 0x80 << 6) ^ ((byte) 0x80))));
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
						char c = (char) ((b1 << 12) ^ (b2 << 6) ^ (b3 ^
							(((byte) 0xE0 << 12) ^ ((byte) 0x80 << 6) ^ ((byte) 0x80))));
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
					int uc = ((b1 << 18) ^
						(b2 << 12) ^
						(b3 << 6) ^
						(b4 ^ (((byte) 0xF0 << 18) ^ ((byte) 0x80 << 12) ^
							((byte) 0x80 << 6) ^ ((byte) 0x80))));
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

	public static String defaultDecodeUTF8(byte[] bytes, int offset, int len) {
		return new String(bytes, offset, len, StandardCharsets.UTF_8);
	}
}
