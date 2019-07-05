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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.runtime.util.StringUtf8Utils;
import org.apache.flink.table.util.SegmentsUtil;
import org.apache.flink.table.utils.EncodingUtils;

import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.dataformat.BinaryString.EMPTY_UTF8;
import static org.apache.flink.table.dataformat.BinaryString.fromAddress;
import static org.apache.flink.table.dataformat.BinaryString.fromBytes;
import static org.apache.flink.table.dataformat.BinaryString.fromString;
import static org.apache.flink.table.dataformat.BinaryString.numBytesForFirstByte;

/**
 * Util for {@link BinaryString}.
 */
public class BinaryStringUtil {

	public static final BinaryString[] EMPTY_STRING_ARRAY = new BinaryString[0];
	private static final List<BinaryString> TRUE_STRINGS =
			Stream.of("t", "true", "y", "yes", "1")
					.map(BinaryString::fromString)
					.peek(BinaryString::ensureMaterialized)
					.collect(Collectors.toList());

	private static final List<BinaryString> FALSE_STRINGS =
			Stream.of("f", "false", "n", "no", "0")
					.map(BinaryString::fromString)
					.peek(BinaryString::ensureMaterialized)
					.collect(Collectors.toList());

	private static byte[] getTmpBytes(BinaryString str, int sizeInBytes) {
		byte[] bytes = SegmentsUtil.allocateReuseBytes(sizeInBytes);
		SegmentsUtil.copyToBytes(str.getSegments(), str.getOffset(), bytes, 0, sizeInBytes);
		return bytes;
	}

	/**
	 * <p>Splits the provided text into an array, separator string specified. </p>
	 *
	 * <p>The separator is not included in the returned String array.
	 * Adjacent separators are treated as separators for empty tokens.</p>
	 *
	 * <p>A {@code null} separator splits on whitespace.</p>
	 *
	 * <pre>
	 * "".splitByWholeSeparatorPreserveAllTokens(*)                 = []
	 * "ab de fg".splitByWholeSeparatorPreserveAllTokens(null)      = ["ab", "de", "fg"]
	 * "ab   de fg".splitByWholeSeparatorPreserveAllTokens(null)    = ["ab", "", "", "de", "fg"]
	 * "ab:cd:ef".splitByWholeSeparatorPreserveAllTokens(":")       = ["ab", "cd", "ef"]
	 * "ab-!-cd-!-ef".splitByWholeSeparatorPreserveAllTokens("-!-") = ["ab", "cd", "ef"]
	 * </pre>
	 *
	 * <p>Note: returned binary strings reuse memory segments from the input str.</p>
	 *
	 * @param separator  String containing the String to be used as a delimiter,
	 *  {@code null} splits on whitespace
	 * @return an array of parsed Strings, {@code null} if null String was input
	 */
	public static BinaryString[] splitByWholeSeparatorPreserveAllTokens(BinaryString str, BinaryString separator) {
		str.ensureMaterialized();
		final int sizeInBytes = str.getSizeInBytes();
		MemorySegment[] segments = str.getSegments();
		int offset = str.getOffset();

		if (sizeInBytes == 0) {
			return EMPTY_STRING_ARRAY;
		}

		if (separator == null || EMPTY_UTF8.equals(separator)) {
			// Split on whitespace.
			return splitByWholeSeparatorPreserveAllTokens(str, fromString(" "));
		}
		separator.ensureMaterialized();

		int sepSize = separator.getSizeInBytes();
		MemorySegment[] sepSegs = separator.getSegments();
		int sepOffset = separator.getOffset();

		final ArrayList<BinaryString> substrings = new ArrayList<>();
		int beg = 0;
		int end = 0;
		while (end < sizeInBytes) {
			end = SegmentsUtil.find(
					segments, offset + beg, sizeInBytes - beg,
					sepSegs, sepOffset, sepSize) - offset;

			if (end > -1) {
				if (end > beg) {

					// The following is OK, because String.substring( beg, end ) excludes
					// the character at the position 'end'.
					substrings.add(fromAddress(segments, offset + beg, end - beg));

					// Set the starting point for the next search.
					// The following is equivalent to beg = end + (separatorLength - 1) + 1,
					// which is the right calculation:
					beg = end + sepSize;
				} else {
					// We found a consecutive occurrence of the separator.
					substrings.add(EMPTY_UTF8);
					beg = end + sepSize;
				}
			} else {
				// String.substring( beg ) goes from 'beg' to the end of the String.
				substrings.add(fromAddress(segments, offset + beg, sizeInBytes - beg));
				end = sizeInBytes;
			}
		}

		return substrings.toArray(new BinaryString[0]);
	}

	/**
	 * Decide boolean representation of a string.
	 */
	public static Boolean toBooleanSQL(BinaryString str) {
		BinaryString lowerCase = str.toLowerCase();
		return TRUE_STRINGS.contains(lowerCase) ? Boolean.TRUE :
				(FALSE_STRINGS.contains(lowerCase) ? Boolean.FALSE : null);
	}

	/**
	 * Calculate the hash value of a given string use {@link MessageDigest}.
	 */
	public static BinaryString hash(BinaryString str, MessageDigest md) {
		return fromString(EncodingUtils.hex(md.digest(str.getBytes())));
	}

	public static BinaryString hash(BinaryString str, String algorithm) throws NoSuchAlgorithmException {
		return hash(str, MessageDigest.getInstance(algorithm));
	}

	/**
	 * Parses this BinaryString to Decimal.
	 *
	 * @return Decimal value if the parsing was successful, or null if overflow
	 * @throws NumberFormatException if the parsing failed.
	 */
	public static Decimal toDecimal(BinaryString str, int precision, int scale) {
		str.ensureMaterialized();

		if (precision > Decimal.MAX_LONG_DIGITS || str.getSizeInBytes() > Decimal.MAX_LONG_DIGITS) {
			return toBigPrecisionDecimal(str, precision, scale);
		}

		int sizeInBytes = str.getSizeInBytes();
		return toDecimalFromBytes(precision, scale, getTmpBytes(str, sizeInBytes), 0, sizeInBytes);
	}

	private static Decimal toDecimalFromBytes(
			int precision, int scale, byte[] bytes, int offset, int sizeInBytes) {
		// Data in Decimal is stored by one long value if `precision` <= Decimal.MAX_LONG_DIGITS.
		// In this case we can directly extract the value from memory segment.
		int i = 0;

		// Remove white spaces at the beginning
		byte b = 0;
		while (i < sizeInBytes) {
			b = bytes[offset + i];
			if (b != ' ' && b != '\n' && b != '\t') {
				break;
			}
			i++;
		}
		if (i == sizeInBytes) {
			// all whitespaces
			return null;
		}

		// ======= begin significant part =======
		final boolean negative = b == '-';
		if (negative || b == '+') {
			i++;
			if (i == sizeInBytes) {
				// only contains prefix plus/minus
				return null;
			}
		}

		long significand = 0;
		int exp = 0;
		int significandLen = 0, pointPos = -1;

		while (i < sizeInBytes) {
			b = bytes[offset + i];
			i++;

			if (b >= '0' && b <= '9') {
				// No need to worry about overflow, because sizeInBytes <= Decimal.MAX_LONG_DIGITS
				significand = significand * 10 + (b - '0');
				significandLen++;
			} else if (b == '.') {
				if (pointPos >= 0) {
					// More than one decimal point
					return null;
				}
				pointPos = significandLen;
			} else {
				break;
			}
		}

		if (pointPos < 0) {
			pointPos = significandLen;
		}
		if (negative) {
			significand = -significand;
		}
		// ======= end significand part =======

		// ======= begin exponential part =======
		if ((b == 'e' || b == 'E') && i < sizeInBytes) {
			b = bytes[offset + i];
			final boolean expNegative = b == '-';
			if (expNegative || b == '+') {
				i++;
				if (i == sizeInBytes) {
					return null;
				}
			}

			int expDigits = 0;
			// As `precision` <= 18, value absolute range is limited to 10^-18 ~ 10^18.
			// The worst case is <18-digits>E-36
			final int expStopValue = 40;

			while (i < sizeInBytes) {
				b = bytes[offset + i];
				i++;

				if (b >= '0' && b <= '9') {
					// No need to worry about larger exponents,
					// because they will produce overflow or underflow
					if (expDigits < expStopValue) {
						expDigits = expDigits * 10 + (b - '0');
					}
				} else {
					break;
				}
			}

			if (expNegative) {
				expDigits = -expDigits;
			}
			exp += expDigits;
		}
		exp -= significandLen - pointPos;
		// ======= end exponential part =======

		// Check for invalid character at the end
		while (i < sizeInBytes) {
			b = bytes[offset + i];
			i++;
			// White spaces are allowed at the end
			if (b != ' ' && b != '\n' && b != '\t') {
				return null;
			}
		}

		// Round exp to scale
		int change = exp + scale;
		if (significandLen + change > precision) {
			// Overflow
			return null;
		}
		if (change >= 0) {
			significand *= Decimal.POW10[change];
		} else {
			int k = negative ? -5 : 5;
			significand = (significand + k * Decimal.POW10[-change - 1]) / Decimal.POW10[-change];
		}
		return Decimal.fromLong(significand, precision, scale);
	}

	private static Decimal toBigPrecisionDecimal(BinaryString str, int precision, int scale) {
		// As data in Decimal is currently stored by BigDecimal if `precision` > Decimal.MAX_LONG_DIGITS,
		// and BigDecimal only supports String or char[] for its constructor,
		// we can't directly extract the value from BinaryString.
		//
		// As BigDecimal(char[], int, int) is faster than BigDecimal(String, int, int),
		// we extract char[] from the memory segment and pass it to the constructor of BigDecimal.
		int sizeInBytes = str.getSizeInBytes();
		int offset = str.getOffset();
		MemorySegment[] segments = str.getSegments();
		char[] chars = SegmentsUtil.allocateReuseChars(sizeInBytes);
		int len;
		if (segments.length == 1) {
			len = StringUtf8Utils.decodeUTF8Strict(segments[0], offset, sizeInBytes, chars);
		} else {
			byte[] bytes = SegmentsUtil.allocateReuseBytes(sizeInBytes);
			SegmentsUtil.copyToBytes(segments, offset, bytes, 0, sizeInBytes);
			len = StringUtf8Utils.decodeUTF8Strict(bytes, 0, sizeInBytes, chars);
		}

		if (len < 0) {
			return null;
		} else {
			// Trim white spaces
			int start = 0, end = len;
			for (int i = 0; i < len; i++) {
				if (chars[i] != ' ' && chars[i] != '\n' && chars[i] != '\t') {
					start = i;
					break;
				}
			}
			for (int i = len - 1; i >= 0; i--) {
				if (chars[i] != ' ' && chars[i] != '\n' && chars[i] != '\t') {
					end = i + 1;
					break;
				}
			}
			try {
				BigDecimal bd = new BigDecimal(chars, start, end - start);
				return Decimal.fromBigDecimal(bd, precision, scale);
			} catch (NumberFormatException nfe) {
				return null;
			}
		}
	}

	/**
	 * Parses this BinaryString to Long.
	 *
	 * <p>Note that, in this method we accumulate the result in negative format, and convert it to
	 * positive format at the end, if this string is not started with '-'. This is because min value
	 * is bigger than max value in digits, e.g. Long.MAX_VALUE is '9223372036854775807' and
	 * Long.MIN_VALUE is '-9223372036854775808'.
	 *
	 * <p>This code is mostly copied from LazyLong.parseLong in Hive.
	 * @return Long value if the parsing was successful else null.
	 */
	public static Long toLong(BinaryString str) {
		int sizeInBytes = str.getSizeInBytes();
		byte[] tmpBytes = getTmpBytes(str, sizeInBytes);
		if (sizeInBytes == 0) {
			return null;
		}
		int i = 0;

		byte b = tmpBytes[i];
		final boolean negative = b == '-';
		if (negative || b == '+') {
			i++;
			if (sizeInBytes == 1) {
				return null;
			}
		}

		long result = 0;
		final byte separator = '.';
		final int radix = 10;
		final long stopValue = Long.MIN_VALUE / radix;
		while (i < sizeInBytes) {
			b = tmpBytes[i];
			i++;
			if (b == separator) {
				// We allow decimals and will return a truncated integral in that case.
				// Therefore we won't throw an exception here (checking the fractional
				// part happens below.)
				break;
			}

			int digit;
			if (b >= '0' && b <= '9') {
				digit = b - '0';
			} else {
				return null;
			}

			// We are going to process the new digit and accumulate the result. However, before
			// doing this, if the result is already smaller than the
			// stopValue(Long.MIN_VALUE / radix), then result * 10 will definitely be smaller
			// than minValue, and we can stop.
			if (result < stopValue) {
				return null;
			}

			result = result * radix - digit;
			// Since the previous result is less than or equal to
			// stopValue(Long.MIN_VALUE / radix), we can just use `result > 0` to check overflow.
			// If result overflows, we should stop.
			if (result > 0) {
				return null;
			}
		}

		// This is the case when we've encountered a decimal separator. The fractional
		// part will not change the number, but we will verify that the fractional part
		// is well formed.
		while (i < sizeInBytes) {
			byte currentByte = tmpBytes[i];
			if (currentByte < '0' || currentByte > '9') {
				return null;
			}
			i++;
		}

		if (!negative) {
			result = -result;
			if (result < 0) {
				return null;
			}
		}
		return result;
	}

	/**
	 * Parses this BinaryString to Int.
	 *
	 * <p>Note that, in this method we accumulate the result in negative format, and convert it to
	 * positive format at the end, if this string is not started with '-'. This is because min value
	 * is bigger than max value in digits, e.g. Integer.MAX_VALUE is '2147483647' and
	 * Integer.MIN_VALUE is '-2147483648'.
	 *
	 * <p>This code is mostly copied from LazyInt.parseInt in Hive.
	 *
	 * <p>Note that, this method is almost same as `toLong`, but we leave it duplicated for performance
	 * reasons, like Hive does.
	 * @return Integer value if the parsing was successful else null.
	 */
	public static Integer toInt(BinaryString str) {
		int sizeInBytes = str.getSizeInBytes();
		byte[] tmpBytes = getTmpBytes(str, sizeInBytes);
		if (sizeInBytes == 0) {
			return null;
		}
		int i = 0;

		byte b = tmpBytes[i];
		final boolean negative = b == '-';
		if (negative || b == '+') {
			i++;
			if (sizeInBytes == 1) {
				return null;
			}
		}

		int result = 0;
		final byte separator = '.';
		final int radix = 10;
		final long stopValue = Integer.MIN_VALUE / radix;
		while (i < sizeInBytes) {
			b = tmpBytes[i];
			i++;
			if (b == separator) {
				// We allow decimals and will return a truncated integral in that case.
				// Therefore we won't throw an exception here (checking the fractional
				// part happens below.)
				break;
			}

			int digit;
			if (b >= '0' && b <= '9') {
				digit = b - '0';
			} else {
				return null;
			}

			// We are going to process the new digit and accumulate the result. However, before
			// doing this, if the result is already smaller than the
			// stopValue(Long.MIN_VALUE / radix), then result * 10 will definitely be smaller
			// than minValue, and we can stop.
			if (result < stopValue) {
				return null;
			}

			result = result * radix - digit;
			// Since the previous result is less than or equal to
			// stopValue(Long.MIN_VALUE / radix), we can just use `result > 0` to check overflow.
			// If result overflows, we should stop.
			if (result > 0) {
				return null;
			}
		}

		// This is the case when we've encountered a decimal separator. The fractional
		// part will not change the number, but we will verify that the fractional part
		// is well formed.
		while (i < sizeInBytes) {
			byte currentByte = tmpBytes[i];
			if (currentByte < '0' || currentByte > '9') {
				return null;
			}
			i++;
		}

		if (!negative) {
			result = -result;
			if (result < 0) {
				return null;
			}
		}
		return result;
	}

	public static Short toShort(BinaryString str) {
		Integer intValue = toInt(str);
		if (intValue != null) {
			short result = intValue.shortValue();
			if (result == intValue) {
				return result;
			}
		}
		return null;
	}

	public static Byte toByte(BinaryString str) {
		Integer intValue = toInt(str);
		if (intValue != null) {
			byte result = intValue.byteValue();
			if (result == intValue) {
				return result;
			}
		}
		return null;
	}

	public static Double toDouble(BinaryString str) {
		try {
			return Double.valueOf(str.toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	public static Float toFloat(BinaryString str) {
		try {
			return Float.valueOf(str.toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * Parse target string as key-value string and
	 * return the value matches key name.
	 * If accept any null arguments, return null.
	 * example:
	 * keyvalue('k1=v1;k2=v2', ';', '=', 'k2') = 'v2'
	 * keyvalue('k1:v1,k2:v2', ',', ':', 'k3') = NULL
	 *
	 * @param split1  separator between key-value tuple.
	 * @param split2  separator between key and value.
	 * @param keyName name of the key whose value you want return.
	 *
	 * @return target value.
	 */
	public static BinaryString keyValue(BinaryString str, byte split1, byte split2, BinaryString keyName) {
		str.ensureMaterialized();
		if (keyName == null || keyName.getSizeInBytes() == 0) {
			return null;
		}
		if (str.inFirstSegment() && keyName.inFirstSegment()) {
			// position in byte
			int byteIdx = 0;
			// position of last split1
			int lastSplit1Idx = -1;
			while (byteIdx < str.sizeInBytes) {
				// If find next split1 in str, process current kv
				if (str.segments[0].get(str.offset + byteIdx) == split1) {
					int currentKeyIdx = lastSplit1Idx + 1;
					// If key of current kv is keyName, return the value directly
					BinaryString value = findValueOfKey(str, split2, keyName, currentKeyIdx, byteIdx);
					if (value != null) {
						return value;
					}
					lastSplit1Idx = byteIdx;
				}
				byteIdx++;
			}
			// process the string which is not ends with split1
			int currentKeyIdx = lastSplit1Idx + 1;
			return findValueOfKey(str, split2, keyName, currentKeyIdx, str.sizeInBytes);
		} else {
			return keyValueSlow(str, split1, split2, keyName);
		}
	}

	private static BinaryString findValueOfKey(
			BinaryString str,
			byte split,
			BinaryString keyName,
			int start,
			int end) {
		int keyNameLen = keyName.sizeInBytes;
		for (int idx = start; idx < end; idx++) {
			if (str.segments[0].get(str.offset + idx) == split) {
				if (idx == start + keyNameLen &&
						str.segments[0].equalTo(keyName.segments[0], str.offset + start,
								keyName.offset, keyNameLen)) {
					int valueIdx = idx + 1;
					int valueLen = end - valueIdx;
					byte[] bytes = new byte[valueLen];
					str.segments[0].get(str.offset + valueIdx, bytes, 0, valueLen);
					return fromBytes(bytes, 0, valueLen);
				} else {
					return null;
				}
			}
		}
		return null;
	}

	private static BinaryString keyValueSlow(
			BinaryString str,
			byte split1,
			byte split2,
			BinaryString keyName) {
		// position in byte
		int byteIdx = 0;
		// position of last split1
		int lastSplit1Idx = -1;
		while (byteIdx < str.sizeInBytes) {
			// If find next split1 in str, process current kv
			if (str.byteAt(byteIdx) == split1) {
				int currentKeyIdx = lastSplit1Idx + 1;
				BinaryString value = findValueOfKeySlow(str, split2, keyName, currentKeyIdx, byteIdx);
				if (value != null) {
					return value;
				}
				lastSplit1Idx = byteIdx;
			}
			byteIdx++;
		}
		int currentKeyIdx = lastSplit1Idx + 1;
		return findValueOfKeySlow(str, split2, keyName, currentKeyIdx, str.sizeInBytes);
	}

	private static BinaryString findValueOfKeySlow(
			BinaryString str,
			byte split,
			BinaryString keyName,
			int start,
			int end) {
		int keyNameLen = keyName.sizeInBytes;
		for (int idx = start; idx < end; idx++) {
			if (str.byteAt(idx) == split) {
				if (idx == start + keyNameLen &&
						SegmentsUtil.equals(str.segments, str.offset + start, keyName.segments,
								keyName.offset, keyNameLen)) {
					int valueIdx = idx + 1;
					byte[] bytes = SegmentsUtil.copyToBytes(str.segments, str.offset + valueIdx, end - valueIdx);
					return fromBytes(bytes);
				} else {
					return null;
				}
			}
		}
		return null;
	}

	public static BinaryString substringSQL(BinaryString str, int pos) {
		return substringSQL(str, pos, Integer.MAX_VALUE);
	}

	public static BinaryString substringSQL(BinaryString str, int pos, int length) {
		if (length < 0) {
			return null;
		}
		str.ensureMaterialized();
		if (str.equals(EMPTY_UTF8)) {
			return EMPTY_UTF8;
		}

		int start;
		int end;
		int numChars = str.numChars();

		if (pos > 0) {
			start = pos - 1;
			if (start >= numChars) {
				return EMPTY_UTF8;
			}
		} else if (pos < 0) {
			start = numChars + pos;
			if (start < 0) {
				return EMPTY_UTF8;
			}
		} else {
			start = 0;
		}

		if ((numChars - start) < length) {
			end = numChars;
		} else {
			end = start + length;
		}
		return str.substring(start, end);
	}

	/**
	 * Concatenates input strings together into a single string.
	 */
	public static BinaryString concat(BinaryString... inputs) {
		return concat(Arrays.asList(inputs));
	}

	/**
	 * Concatenates input strings together into a single string.
	 */
	public static BinaryString concat(Iterable<BinaryString> inputs) {
		// Compute the total length of the result.
		int totalLength = 0;
		for (BinaryString input : inputs) {
			if (input != null) {
				input.ensureMaterialized();
				totalLength += input.getSizeInBytes();
			}
		}

		// Allocate a new byte array, and copy the inputs one by one into it.
		final byte[] result = new byte[totalLength];
		int offset = 0;
		for (BinaryString input : inputs) {
			if (input != null) {
				int len = input.sizeInBytes;
				SegmentsUtil.copyToBytes(input.segments, input.offset, result, offset, len);
				offset += len;
			}
		}
		return fromBytes(result);
	}

	/**
	 * Concatenates input strings together into a single string using the separator.
	 * A null input is skipped. For example, concat(",", "a", null, "c") would yield "a,c".
	 */
	public static BinaryString concatWs(BinaryString separator, BinaryString... inputs) {
		return concatWs(separator, Arrays.asList(inputs));
	}

	/**
	 * Concatenates input strings together into a single string using the separator.
	 * A null input is skipped. For example, concat(",", "a", null, "c") would yield "a,c".
	 */
	public static BinaryString concatWs(BinaryString separator, Iterable<BinaryString> inputs) {
		if (null == separator || EMPTY_UTF8.equals(separator)) {
			return concat(inputs);
		}
		separator.ensureMaterialized();

		int numInputBytes = 0;  // total number of bytes from the inputs
		int numInputs = 0;      // number of non-null inputs
		for (BinaryString input : inputs) {
			if (input != null) {
				input.ensureMaterialized();
				numInputBytes += input.sizeInBytes;
				numInputs++;
			}
		}

		if (numInputs == 0) {
			// Return an empty string if there is no input, or all the inputs are null.
			return EMPTY_UTF8;
		}

		// Allocate a new byte array, and copy the inputs one by one into it.
		// The size of the new array is the size of all inputs, plus the separators.
		final byte[] result = new byte[numInputBytes + (numInputs - 1) * separator.sizeInBytes];
		int offset = 0;

		int j = 0;
		for (BinaryString input : inputs) {
			if (input != null) {
				int len = input.sizeInBytes;
				SegmentsUtil.copyToBytes(input.segments, input.offset, result, offset, len);
				offset += len;

				j++;
				// Add separator if this is not the last input.
				if (j < numInputs) {
					SegmentsUtil.copyToBytes(separator.segments, separator.offset, result, offset, separator.sizeInBytes);
					offset += separator.sizeInBytes;
				}
			}
		}
		return fromBytes(result);
	}

	/**
	 * Reverse each character in current string.
	 *
	 * @return a new string which character order is reverse to current string.
	 */
	public static BinaryString reverse(BinaryString str) {
		str.ensureMaterialized();
		if (str.inFirstSegment()) {
			byte[] result = new byte[str.sizeInBytes];
			// position in byte
			int byteIdx = 0;
			while (byteIdx < str.sizeInBytes) {
				int charBytes = numBytesForFirstByte(str.getByteOneSegment(byteIdx));
				str.segments[0].get(
						str.offset + byteIdx,
						result,
						result.length - byteIdx - charBytes,
						charBytes);
				byteIdx += charBytes;
			}
			return BinaryString.fromBytes(result);
		} else {
			return reverseMultiSegs(str);
		}
	}

	private static BinaryString reverseMultiSegs(BinaryString str) {
		byte[] result = new byte[str.sizeInBytes];
		// position in byte
		int byteIdx = 0;
		int segSize = str.segments[0].size();
		BinaryString.SegmentAndOffset index = str.firstSegmentAndOffset(segSize);
		while (byteIdx <  str.sizeInBytes) {
			int charBytes = numBytesForFirstByte(index.value());
			SegmentsUtil.copyMultiSegmentsToBytes(
					str.segments,
					str.offset + byteIdx,
					result,
					result.length - byteIdx - charBytes,
					charBytes);
			byteIdx += charBytes;
			index.skipBytes(charBytes, segSize);
		}
		return BinaryString.fromBytes(result);
	}

	/**
	 * Walk each character of current string from both ends, remove the character if it
	 * is in trim string.
	 * Return the new substring which both ends trim characters have been removed.
	 *
	 * @param trimStr the trim string
	 * @return A subString which both ends trim characters have been removed.
	 */
	public static BinaryString trim(BinaryString str, BinaryString trimStr) {
		if (trimStr == null) {
			return null;
		}
		return trimRight(trimLeft(str, trimStr), trimStr);
	}

	public static BinaryString trimLeft(BinaryString str) {
		str.ensureMaterialized();
		if (str.inFirstSegment()) {
			int s = 0;
			// skip all of the space (0x20) in the left side
			while (s < str.sizeInBytes && str.getByteOneSegment(s) == 0x20) {
				s++;
			}
			if (s == str.sizeInBytes) {
				// empty string
				return EMPTY_UTF8;
			} else {
				return str.copyBinaryStringInOneSeg(s, str.sizeInBytes - s);
			}
		} else {
			return trimLeftSlow(str);
		}
	}

	private static BinaryString trimLeftSlow(BinaryString str) {
		int s = 0;
		int segSize = str.segments[0].size();
		BinaryString.SegmentAndOffset front = str.firstSegmentAndOffset(segSize);
		// skip all of the space (0x20) in the left side
		while (s < str.sizeInBytes && front.value() == 0x20) {
			s++;
			front.nextByte(segSize);
		}
		if (s == str.sizeInBytes) {
			// empty string
			return EMPTY_UTF8;
		} else {
			return str.copyBinaryString(s, str.sizeInBytes - 1);
		}
	}

	public static boolean isSpaceString(BinaryString str) {
		if (str.javaObject != null) {
			return str.javaObject.equals(" ");
		} else {
			return str.byteAt(0) == ' ';
		}
	}

	/**
	 * Walk each character of current string from left end, remove the character if it
	 * is in trim string. Stops at the first character which is not in trim string.
	 * Return the new substring.
	 *
	 * @param trimStr the trim string
	 * @return A subString which removes all of the character from the left side that is in
	 * trim string.
	 */
	public static BinaryString trimLeft(BinaryString str, BinaryString trimStr) {
		str.ensureMaterialized();
		if (trimStr == null) {
			return null;
		}
		trimStr.ensureMaterialized();
		if (isSpaceString(trimStr)) {
			return trimLeft(str);
		}
		if (str.inFirstSegment()) {
			int searchIdx = 0;
			while (searchIdx < str.sizeInBytes) {
				int charBytes = numBytesForFirstByte(str.getByteOneSegment(searchIdx));
				BinaryString currentChar = str.copyBinaryStringInOneSeg(searchIdx, charBytes);
				// try to find the matching for the character in the trimString characters.
				if (trimStr.contains(currentChar)) {
					searchIdx += charBytes;
				} else {
					break;
				}
			}
			// empty string
			if (searchIdx >= str.sizeInBytes) {
				return EMPTY_UTF8;
			} else {
				return str.copyBinaryStringInOneSeg(searchIdx, str.sizeInBytes - searchIdx);
			}
		} else {
			return trimLeftSlow(str, trimStr);
		}
	}

	private static BinaryString trimLeftSlow(BinaryString str, BinaryString trimStr) {
		int searchIdx = 0;
		int segSize = str.segments[0].size();
		BinaryString.SegmentAndOffset front = str.firstSegmentAndOffset(segSize);
		while (searchIdx < str.sizeInBytes) {
			int charBytes = numBytesForFirstByte(front.value());
			BinaryString currentChar = str.copyBinaryString(searchIdx, searchIdx + charBytes - 1);
			if (trimStr.contains(currentChar)) {
				searchIdx += charBytes;
				front.skipBytes(charBytes, segSize);
			} else {
				break;
			}
		}
		if (searchIdx == str.sizeInBytes) {
			// empty string
			return EMPTY_UTF8;
		} else {
			return str.copyBinaryString(searchIdx, str.sizeInBytes - 1);
		}
	}

	public static BinaryString trimRight(BinaryString str) {
		str.ensureMaterialized();
		if (str.inFirstSegment()) {
			int e = str.sizeInBytes - 1;
			// skip all of the space (0x20) in the right side
			while (e >= 0 && str.getByteOneSegment(e) == 0x20) {
				e--;
			}

			if (e < 0) {
				// empty string
				return EMPTY_UTF8;
			} else {
				return str.copyBinaryStringInOneSeg(0, e + 1);
			}
		} else {
			return trimRightSlow(str);
		}
	}

	private static BinaryString trimRightSlow(BinaryString str) {
		int e = str.sizeInBytes - 1;
		int segSize = str.segments[0].size();
		BinaryString.SegmentAndOffset behind = str.lastSegmentAndOffset(segSize);
		// skip all of the space (0x20) in the right side
		while (e >= 0 && behind.value() == 0x20) {
			e--;
			behind.previousByte(segSize);
		}

		if (e < 0) {
			// empty string
			return EMPTY_UTF8;
		} else {
			return str.copyBinaryString(0, e);
		}
	}

	/**
	 * Walk each character of current string from right end, remove the character if it
	 * is in trim string. Stops at the first character which is not in trim string.
	 * Return the new substring.
	 *
	 * @param trimStr the trim string
	 * @return A subString which removes all of the character from the right side that is in
	 * trim string.
	 */
	public static BinaryString trimRight(BinaryString str, BinaryString trimStr) {
		str.ensureMaterialized();
		if (trimStr == null) {
			return null;
		}
		trimStr.ensureMaterialized();
		if (isSpaceString(trimStr)) {
			return trimRight(str);
		}
		if (str.inFirstSegment()) {
			int charIdx = 0;
			int byteIdx = 0;
			// each element in charLens is length of character in the source string
			int[] charLens = new int[str.sizeInBytes];
			// each element in charStartPos is start position of first byte in the source string
			int[] charStartPos = new int[str.sizeInBytes];
			while (byteIdx < str.sizeInBytes) {
				charStartPos[charIdx] = byteIdx;
				charLens[charIdx] = numBytesForFirstByte(str.getByteOneSegment(byteIdx));
				byteIdx += charLens[charIdx];
				charIdx++;
			}
			// searchIdx points to the first character which is not in trim string from the right
			// end.
			int searchIdx = str.sizeInBytes - 1;
			charIdx -= 1;
			while (charIdx >= 0) {
				BinaryString currentChar = str.copyBinaryStringInOneSeg(
						charStartPos[charIdx], charLens[charIdx]);
				if (trimStr.contains(currentChar)) {
					searchIdx -= charLens[charIdx];
				} else {
					break;
				}
				charIdx--;
			}
			if (searchIdx < 0) {
				// empty string
				return EMPTY_UTF8;
			} else {
				return str.copyBinaryStringInOneSeg(0, searchIdx + 1);
			}
		} else {
			return trimRightSlow(str, trimStr);
		}
	}

	private static BinaryString trimRightSlow(BinaryString str, BinaryString trimStr) {
		int charIdx = 0;
		int byteIdx = 0;
		int segSize = str.segments[0].size();
		BinaryString.SegmentAndOffset index = str.firstSegmentAndOffset(segSize);
		// each element in charLens is length of character in the source string
		int[] charLens = new int[str.sizeInBytes];
		// each element in charStartPos is start position of first byte in the source string
		int[] charStartPos = new int[str.sizeInBytes];
		while (byteIdx < str.sizeInBytes) {
			charStartPos[charIdx] = byteIdx;
			int charBytes = numBytesForFirstByte(index.value());
			charLens[charIdx] = charBytes;
			byteIdx += charBytes;
			charIdx++;
			index.skipBytes(charBytes, segSize);
		}
		// searchIdx points to the first character which is not in trim string from the right
		// end.
		int searchIdx = str.sizeInBytes - 1;
		charIdx -= 1;
		while (charIdx >= 0) {
			BinaryString currentChar = str.copyBinaryString(
					charStartPos[charIdx],
					charStartPos[charIdx] + charLens[charIdx] - 1);
			if (trimStr.contains(currentChar)) {
				searchIdx -= charLens[charIdx];
			} else {
				break;
			}
			charIdx--;
		}
		if (searchIdx < 0) {
			// empty string
			return EMPTY_UTF8;
		} else {
			return str.copyBinaryString(0, searchIdx);
		}
	}

	public static BinaryString trim(BinaryString str, boolean leading, boolean trailing, BinaryString seek) {
		str.ensureMaterialized();
		if (seek == null) {
			return null;
		}
		if (leading && trailing) {
			return trim(str, seek);
		} else if (leading) {
			return trimLeft(str, seek);
		} else if (trailing) {
			return trimRight(str, seek);
		} else {
			return str;
		}
	}

	public static String safeToString(BinaryString str) {
		if (str == null) {
			return null;
		} else {
			return str.toString();
		}
	}
}
