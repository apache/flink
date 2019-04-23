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

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.runtime.util.StringUtf8Utils;
import org.apache.flink.table.typeutils.BinaryStringTypeInfoFactory;
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

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A utf8 string which is backed by {@link MemorySegment} instead of String. Its data may span
 * multiple {@link MemorySegment}s.
 *
 * <p>Used for internal table-level implementation. The built-in operator will use it for comparison,
 * search, and so on.
 *
 * <p>{@code BinaryString} are influenced by Apache Spark UTF8String.
 */
@TypeInfo(BinaryStringTypeInfoFactory.class)
public final class BinaryString extends LazyBinaryFormat<String> implements Comparable<BinaryString> {

	public static final BinaryString EMPTY_UTF8 = BinaryString.fromBytes(StringUtf8Utils.encodeUTF8(""));
	protected static final BinaryString[] EMPTY_STRING_ARRAY = new BinaryString[0];

	public BinaryString(MemorySegment[] segments, int offset, int sizeInBytes) {
		super(segments, offset, sizeInBytes);
	}

	public BinaryString(String javaObject) {
		super(javaObject);
	}

	public BinaryString(MemorySegment[] segments, int offset, int sizeInBytes, String javaObject) {
		super(segments, offset, sizeInBytes, javaObject);
	}

	/**
	 * Creates an BinaryString from given address (base and offset) and length.
	 */
	public static BinaryString fromAddress(
			MemorySegment[] segments, int offset, int numBytes) {
		return new BinaryString(segments, offset, numBytes);
	}

	public static BinaryString fromString(String str) {
		if (str == null) {
			return null;
		} else {
			return new BinaryString(str);
		}
	}

	public static BinaryString fromBytes(byte[] bytes) {
		return fromBytes(bytes, 0, bytes.length);
	}

	public static BinaryString fromBytes(byte[] bytes, int offset, int numBytes) {
		return new BinaryString(
			new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, offset, numBytes);
	}

	/**
	 * Creates an BinaryString that contains `length` spaces.
	 */
	public static BinaryString blankString(int length) {
		byte[] spaces = new byte[length];
		Arrays.fill(spaces, (byte) ' ');
		return fromBytes(spaces);
	}

	/**
	 * Returns the number of bytes for a code point with the first byte as `b`.
	 * @param b The first byte of a code point
	 */
	private static int numBytesForFirstByte(final byte b) {
		if (b >= 0) {
			// 1 byte, 7 bits: 0xxxxxxx
			return 1;
		} else if ((b >> 5) == -2 && (b & 0x1e) != 0) {
			// 2 bytes, 11 bits: 110xxxxx 10xxxxxx
			return 2;
		} else if ((b >> 4) == -2) {
			// 3 bytes, 16 bits: 1110xxxx 10xxxxxx 10xxxxxx
			return 3;
		} else if ((b >> 3) == -2) {
			// 4 bytes, 21 bits: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
			return 4;
		} else {
			// throw new IllegalArgumentException();
			// Skip the first byte disallowed in UTF-8
			return 1;
		}
	}

	/**
	 * Returns the number of code points in it.
	 */
	public int numChars() {
		ensureMaterialized();
		if (inFirstSegment()) {
			int len = 0;
			for (int i = 0; i < sizeInBytes; i += numBytesForFirstByte(getByteOneSegment(i))) {
				len++;
			}
			return len;
		} else {
			return numCharsSlow();
		}
	}

	private int numCharsSlow() {
		int len = 0;
		int segSize = segments[0].size();
		SegmentAndOffset index = firstSegmentAndOffset(segSize);
		int i = 0;
		while (i < sizeInBytes) {
			int charBytes = numBytesForFirstByte(index.value());
			i += charBytes;
			len++;
			index.skipBytes(charBytes, segSize);
		}
		return len;
	}

	public boolean isSpaceString() {
		if (javaObject != null) {
			return javaObject.equals(" ");
		} else {
			return getByte(0) == ' ';
		}
	}

	public byte getByte(int i) {
		ensureMaterialized();
		int globalOffset = offset + i;
		int size = segments[0].size();
		if (globalOffset < size) {
			return segments[0].get(globalOffset);
		} else {
			return segments[globalOffset / size].get(globalOffset % size);
		}
	}

	private byte getByteOneSegment(int i) {
		return segments[0].get(offset + i);
	}

	/**
	 * Get the underlying byte array, the returned bytes maybe reused.
	 */
	public byte[] getBytes() {
		ensureMaterialized();
		return SegmentsUtil.getBytes(segments, offset, sizeInBytes);
	}

	@Override
	public boolean equals(Object o) {
		if (o != null && o instanceof BinaryString) {
			BinaryString other = (BinaryString) o;
			if (javaObject != null && other.javaObject != null) {
				return javaObject.equals(other.javaObject);
			}

			ensureMaterialized();
			other.ensureMaterialized();
			return binaryEquals(other);
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		if (javaObject == null) {
			byte[] bytes = SegmentsUtil.allocateReuseBytes(sizeInBytes);
			SegmentsUtil.copyToBytes(segments, offset, bytes, 0, sizeInBytes);
			javaObject = StringUtf8Utils.decodeUTF8(bytes, 0, sizeInBytes);
		}
		return javaObject;
	}

	@Override
	public void materialize() {
		byte[] bytes = StringUtf8Utils.encodeUTF8(javaObject);
		segments = new MemorySegment[] {MemorySegmentFactory.wrap(bytes)};
		offset = 0;
		sizeInBytes = bytes.length;
	}

	public BinaryString copy() {
		ensureMaterialized();
		byte[] copy = SegmentsUtil.copyToBytes(segments, offset, sizeInBytes);
		return new BinaryString(new MemorySegment[] {MemorySegmentFactory.wrap(copy)},
				0, sizeInBytes, javaObject);
	}

	/**
	 * UTF-8 supports bytes comparison.
	 */
	@Override
	public int compareTo(BinaryString other) {

		if (javaObject != null && other.javaObject != null) {
			return javaObject.compareTo(other.javaObject);
		}

		ensureMaterialized();
		other.ensureMaterialized();
		if (segments.length == 1 && other.segments.length == 1) {

			int len = Math.min(sizeInBytes, other.sizeInBytes);
			MemorySegment seg1 = segments[0];
			MemorySegment seg2 = other.segments[0];

			for (int i = 0; i < len; i++) {
				int res = (seg1.get(offset + i) & 0xFF) - (seg2.get(other.offset + i) & 0xFF);
				if (res != 0) {
					return res;
				}
			}
			return sizeInBytes - other.sizeInBytes;
		}

		// if there are multi segments.
		return compareMultiSegments(other);
	}

	/**
	 * Find the boundaries of segments, and then compare MemorySegment.
	 */
	private int compareMultiSegments(BinaryString other) {

		if (sizeInBytes == 0 || other.sizeInBytes == 0) {
			return sizeInBytes - other.sizeInBytes;
		}

		int len = Math.min(sizeInBytes, other.sizeInBytes);

		MemorySegment seg1 = segments[0];
		MemorySegment seg2 = other.segments[0];

		int segmentSize = segments[0].size();
		int otherSegmentSize = other.segments[0].size();

		int sizeOfFirst1 = segmentSize - offset;
		int sizeOfFirst2 = otherSegmentSize - other.offset;

		int varSegIndex1 = 1;
		int varSegIndex2 = 1;

		// find the first segment of this string.
		while (sizeOfFirst1 <= 0) {
			sizeOfFirst1 += segmentSize;
			seg1 = segments[varSegIndex1++];
		}

		while (sizeOfFirst2 <= 0) {
			sizeOfFirst2 += otherSegmentSize;
			seg2 = other.segments[varSegIndex2++];
		}

		int offset1 = segmentSize - sizeOfFirst1;
		int offset2 = otherSegmentSize - sizeOfFirst2;

		int needCompare = Math.min(Math.min(sizeOfFirst1, sizeOfFirst2), len);

		while (needCompare > 0) {
			// compare in one segment.
			for (int i = 0; i < needCompare; i++) {
				int res = (seg1.get(offset1 + i) & 0xFF) - (seg2.get(offset2 + i) & 0xFF);
				if (res != 0) {
					return res;
				}
			}
			if (needCompare == len) {
				break;
			}
			len -= needCompare;
			// next segment
			if (sizeOfFirst1 < sizeOfFirst2) { //I am smaller
				seg1 = segments[varSegIndex1++];
				offset1 = 0;
				offset2 += needCompare;
				sizeOfFirst1 = segmentSize;
				sizeOfFirst2 -= needCompare;
			} else if (sizeOfFirst1 > sizeOfFirst2) { //other is smaller
				seg2 = other.segments[varSegIndex2++];
				offset2 = 0;
				offset1 += needCompare;
				sizeOfFirst2 = otherSegmentSize;
				sizeOfFirst1 -= needCompare;
			} else { // same, should go ahead both.
				seg1 = segments[varSegIndex1++];
				seg2 = other.segments[varSegIndex2++];
				offset1 = 0;
				offset2 = 0;
				sizeOfFirst1 = segmentSize;
				sizeOfFirst2 = otherSegmentSize;
			}
			needCompare = Math.min(Math.min(sizeOfFirst1, sizeOfFirst2), len);
		}

		checkArgument(needCompare == len);

		return sizeInBytes - other.sizeInBytes;
	}

	public static String safeToString(BinaryString str) {
		if (str == null) {
			return null;
		} else {
			return str.toString();
		}
	}

	private boolean inFirstSegment() {
		return sizeInBytes + offset <= segments[0].size();
	}


	// ------------------------------------------------------------------------------------------
	// Utility methods on BinaryString
	// ------------------------------------------------------------------------------------------

	public BinaryString substringSQL(int pos) {
		return substringSQL(pos, Integer.MAX_VALUE);
	}

	public BinaryString substringSQL(int pos, int length) {
		if (length < 0) {
			return null;
		}
		ensureMaterialized();
		if (equals(EMPTY_UTF8)) {
			return EMPTY_UTF8;
		}

		int start;
		int end;
		int numChars = numChars();

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
		return substring(start, end);
	}

	/**
	 * Returns a substring of this.
	 * @param start the position of first code point
	 * @param until the position after last code point, exclusive.
	 */
	public BinaryString substring(final int start, final int until) {
		ensureMaterialized();
		if (until <= start || start >= sizeInBytes) {
			return EMPTY_UTF8;
		}
		if (inFirstSegment()) {
			MemorySegment segment = segments[0];
			int i = 0;
			int c = 0;
			while (i < sizeInBytes && c < start) {
				i += numBytesForFirstByte(segment.get(i + offset));
				c += 1;
			}

			int j = i;
			while (i < sizeInBytes && c < until) {
				i += numBytesForFirstByte(segment.get(i + offset));
				c += 1;
			}

			if (i > j) {
				byte[] bytes = new byte[i - j];
				segment.get(offset + j, bytes, 0, i - j);
				return fromBytes(bytes);
			} else {
				return EMPTY_UTF8;
			}
		} else {
			return substringSlow(start, until);
		}
	}

	private BinaryString substringSlow(final int start, final int until) {
		int segSize = segments[0].size();
		SegmentAndOffset index = firstSegmentAndOffset(segSize);
		int i = 0;
		int c = 0;
		while (i < sizeInBytes && c < start) {
			int charSize = numBytesForFirstByte(index.value());
			i += charSize;
			index.skipBytes(charSize, segSize);
			c += 1;
		}

		int j = i;
		while (i < sizeInBytes && c < until) {
			int charSize = numBytesForFirstByte(index.value());
			i += charSize;
			index.skipBytes(charSize, segSize);
			c += 1;
		}

		if (i > j) {
			return fromBytes(SegmentsUtil.copyToBytes(segments, offset + j, i - j));
		} else {
			return EMPTY_UTF8;
		}
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
	 * Returns whether this contains `substring` or not.
	 * Same to like '%substring%'.
	 */
	public boolean contains(final BinaryString substring) {
		ensureMaterialized();
		substring.ensureMaterialized();
		if (substring.sizeInBytes == 0) {
			return true;
		}
		int find = SegmentsUtil.find(
			segments, offset, sizeInBytes,
			substring.segments, substring.offset, substring.sizeInBytes);
		return find != -1;
	}

	/**
	 * Same to like 'prefix%'.
	 */
	public boolean startsWith(final BinaryString prefix) {
		ensureMaterialized();
		prefix.ensureMaterialized();
		return matchAt(prefix, 0);
	}

	/**
	 * Same to like '%suffix'.
	 */
	public boolean endsWith(final BinaryString suffix) {
		ensureMaterialized();
		suffix.ensureMaterialized();
		return matchAt(suffix, sizeInBytes - suffix.sizeInBytes);
	}

	private boolean matchAt(final BinaryString s, int pos) {
		return (inFirstSegment() && s.inFirstSegment()) ? matchAtOneSeg(s, pos) : matchAtVarSeg(s, pos);
	}

	private boolean matchAtOneSeg(final BinaryString s, int pos) {
		return s.sizeInBytes + pos <= sizeInBytes && pos >= 0 &&
			segments[0].equalTo(s.segments[0], offset + pos, s.offset, s.sizeInBytes);
	}

	private boolean matchAtVarSeg(final BinaryString s, int pos) {
		return s.sizeInBytes + pos <= sizeInBytes && pos >= 0 &&
			SegmentsUtil.equals(segments, offset + pos, s.segments, s.offset, s.sizeInBytes);
	}

	private BinaryString copyBinaryStringInOneSeg(int start, int end) {
		int len = end - start + 1;
		byte[] newBytes = new byte[len];
		segments[0].get(offset + start, newBytes, 0, len);
		return fromBytes(newBytes);
	}

	private BinaryString copyBinaryString(int start, int end) {
		int len = end - start + 1;
		byte[] newBytes = new byte[len];
		SegmentsUtil.copyToBytes(segments, offset + start, newBytes, 0, len);
		return fromBytes(newBytes);
	}

	public BinaryString trim() {
		ensureMaterialized();
		if (inFirstSegment()) {
			int s = 0;
			int e = this.sizeInBytes - 1;
			// skip all of the space (0x20) in the left side
			while (s < this.sizeInBytes && getByteOneSegment(s) == 0x20) {
				s++;
			}
			// skip all of the space (0x20) in the right side
			while (e >= s && getByteOneSegment(e) == 0x20) {
				e--;
			}
			if (s > e) {
				// empty string
				return EMPTY_UTF8;
			} else {
				return copyBinaryStringInOneSeg(s, e);
			}
		} else {
			return trimSlow();
		}
	}

	private BinaryString trimSlow() {
		int s = 0;
		int e = this.sizeInBytes - 1;
		int segSize = segments[0].size();
		SegmentAndOffset front = firstSegmentAndOffset(segSize);
		// skip all of the space (0x20) in the left side
		while (s < this.sizeInBytes && front.value() == 0x20) {
			s++;
			front.nextByte(segSize);
		}
		SegmentAndOffset behind = lastSegmentAndOffset(segSize);
		// skip all of the space (0x20) in the right side
		while (e >= s && behind.value() == 0x20) {
			e--;
			behind.previousByte(segSize);
		}
		if (s > e) {
			// empty string
			return EMPTY_UTF8;
		} else {
			return copyBinaryString(s, e);
		}
	}

	/**
	 * Walk each character of current string from both ends, remove the character if it
	 * is in trim string.
	 * Return the new substring which both ends trim characters have been removed.
	 *
	 * @param trimStr the trim string
	 * @return A subString which both ends trim characters have been removed.
	 */
	public BinaryString trim(BinaryString trimStr) {
		if (trimStr == null) {
			return null;
		}
		return trimLeft(trimStr).trimRight(trimStr);
	}

	public BinaryString trimLeft() {
		ensureMaterialized();
		if (inFirstSegment()) {
			int s = 0;
			// skip all of the space (0x20) in the left side
			while (s < this.sizeInBytes && getByteOneSegment(s) == 0x20) {
				s++;
			}
			if (s == this.sizeInBytes) {
				// empty string
				return EMPTY_UTF8;
			} else {
				return copyBinaryStringInOneSeg(s, this.sizeInBytes - 1);
			}
		} else {
			return trimLeftSlow();
		}
	}

	private BinaryString trimLeftSlow() {
		int s = 0;
		int segSize = segments[0].size();
		SegmentAndOffset front = firstSegmentAndOffset(segSize);
		// skip all of the space (0x20) in the left side
		while (s < this.sizeInBytes && front.value() == 0x20) {
			s++;
			front.nextByte(segSize);
		}
		if (s == this.sizeInBytes) {
			// empty string
			return EMPTY_UTF8;
		} else {
			return copyBinaryString(s, this.sizeInBytes - 1);
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
	public BinaryString trimLeft(BinaryString trimStr) {
		ensureMaterialized();
		if (trimStr == null) {
			return null;
		}
		trimStr.ensureMaterialized();
		if (trimStr.isSpaceString()) {
			return trimLeft();
		}
		if (inFirstSegment()) {
			int searchIdx = 0;
			while (searchIdx < this.sizeInBytes) {
				int charBytes = numBytesForFirstByte(getByteOneSegment(searchIdx));
				BinaryString currentChar = copyBinaryStringInOneSeg(searchIdx,
					searchIdx + charBytes - 1);
				// try to find the matching for the character in the trimString characters.
				if (trimStr.contains(currentChar)) {
					searchIdx += charBytes;
				} else {
					break;
				}
			}
			// empty string
			if (searchIdx >= sizeInBytes) {
				return EMPTY_UTF8;
			} else {
				return copyBinaryStringInOneSeg(searchIdx, sizeInBytes - 1);
			}
		} else {
			return trimLeftSlow(trimStr);
		}
	}

	private BinaryString trimLeftSlow(BinaryString trimStr) {
		int searchIdx = 0;
		int segSize = segments[0].size();
		SegmentAndOffset front = firstSegmentAndOffset(segSize);
		while (searchIdx < this.sizeInBytes) {
			int charBytes = numBytesForFirstByte(front.value());
			BinaryString currentChar = copyBinaryString(searchIdx, searchIdx + charBytes - 1);
			if (trimStr.contains(currentChar)) {
				searchIdx += charBytes;
				front.skipBytes(charBytes, segSize);
			} else {
				break;
			}
		}
		if (searchIdx == this.sizeInBytes) {
			// empty string
			return EMPTY_UTF8;
		} else {
			return copyBinaryString(searchIdx, this.sizeInBytes - 1);
		}
	}

	public BinaryString trimRight() {
		ensureMaterialized();
		if (inFirstSegment()) {
			int e = sizeInBytes - 1;
			// skip all of the space (0x20) in the right side
			while (e >= 0 && getByteOneSegment(e) == 0x20) {
				e--;
			}

			if (e < 0) {
				// empty string
				return EMPTY_UTF8;
			} else {
				return copyBinaryStringInOneSeg(0, e);
			}
		} else {
			return trimRightSlow();
		}
	}

	private BinaryString trimRightSlow() {
		int e = sizeInBytes - 1;
		int segSize = segments[0].size();
		SegmentAndOffset behind = lastSegmentAndOffset(segSize);
		// skip all of the space (0x20) in the right side
		while (e >= 0 && behind.value() == 0x20) {
			e--;
			behind.previousByte(segSize);
		}

		if (e < 0) {
			// empty string
			return EMPTY_UTF8;
		} else {
			return copyBinaryString(0, e);
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
	public BinaryString trimRight(BinaryString trimStr) {
		ensureMaterialized();
		if (trimStr == null) {
			return null;
		}
		trimStr.ensureMaterialized();
		if (trimStr.isSpaceString()) {
			return trimRight();
		}
		if (inFirstSegment()) {
			int charIdx = 0;
			int byteIdx = 0;
			// each element in charLens is length of character in the source string
			int[] charLens = new int[sizeInBytes];
			// each element in charStartPos is start position of first byte in the source string
			int[] charStartPos = new int[sizeInBytes];
			while (byteIdx < sizeInBytes) {
				charStartPos[charIdx] = byteIdx;
				charLens[charIdx] = numBytesForFirstByte(getByteOneSegment(byteIdx));
				byteIdx += charLens[charIdx];
				charIdx++;
			}
			// searchIdx points to the first character which is not in trim string from the right
			// end.
			int searchIdx = sizeInBytes - 1;
			charIdx -= 1;
			while (charIdx >= 0) {
				BinaryString currentChar = copyBinaryStringInOneSeg(
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
				return copyBinaryStringInOneSeg(0, searchIdx);
			}
		} else {
			return trimRightSlow(trimStr);
		}
	}

	private BinaryString trimRightSlow(BinaryString trimStr) {
		int charIdx = 0;
		int byteIdx = 0;
		int segSize = segments[0].size();
		SegmentAndOffset index = firstSegmentAndOffset(segSize);
		// each element in charLens is length of character in the source string
		int[] charLens = new int[sizeInBytes];
		// each element in charStartPos is start position of first byte in the source string
		int[] charStartPos = new int[sizeInBytes];
		while (byteIdx < sizeInBytes) {
			charStartPos[charIdx] = byteIdx;
			int charBytes = numBytesForFirstByte(index.value());
			charLens[charIdx] = charBytes;
			byteIdx += charBytes;
			charIdx++;
			index.skipBytes(charBytes, segSize);
		}
		// searchIdx points to the first character which is not in trim string from the right
		// end.
		int searchIdx = sizeInBytes - 1;
		charIdx -= 1;
		while (charIdx >= 0) {
			BinaryString currentChar = copyBinaryString(
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
			return copyBinaryString(0, searchIdx);
		}
	}

	public BinaryString trim(boolean leading, boolean trailing, BinaryString seek) {
		ensureMaterialized();
		if (seek == null) {
			return null;
		}
		if (leading && trailing) {
			return trim(seek);
		} else if (leading) {
			return trimLeft(seek);
		} else if (trailing) {
			return trimRight(seek);
		} else {
			return this;
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
	public BinaryString keyValue(byte split1, byte split2, BinaryString keyName) {
		ensureMaterialized();
		if (keyName == null || keyName.getSizeInBytes() == 0) {
			return null;
		}
		if (inFirstSegment() && keyName.inFirstSegment()) {
			// position in byte
			int byteIdx = 0;
			// position of last split1
			int lastSplit1Idx = -1;
			while (byteIdx < sizeInBytes) {
				// If find next split1 in str, process current kv
				if (segments[0].get(offset + byteIdx) == split1) {
					int currentKeyIdx = lastSplit1Idx + 1;
					// If key of current kv is keyName, return the value directly
					BinaryString value = findValueOfKey(split2, keyName, currentKeyIdx, byteIdx);
					if (value != null) {
						return value;
					}
					lastSplit1Idx = byteIdx;
				}
				byteIdx++;
			}
			// process the string which is not ends with split1
			int currentKeyIdx = lastSplit1Idx + 1;
			return findValueOfKey(split2, keyName, currentKeyIdx, sizeInBytes);
		} else {
			return keyValueSlow(split1, split2, keyName);
		}
	}

	private BinaryString findValueOfKey(
		byte split,
		BinaryString keyName,
		int start,
		int end) {
		int keyNameLen = keyName.sizeInBytes;
		for (int idx = start; idx < end; idx++) {
			if (segments[0].get(offset + idx) == split) {
				if (idx == start + keyNameLen &&
					segments[0].equalTo(keyName.segments[0], offset + start,
						keyName.offset, keyNameLen)) {
					int valueIdx = idx + 1;
					int valueLen = end - valueIdx;
					byte[] bytes = new byte[valueLen];
					segments[0].get(offset + valueIdx, bytes, 0, valueLen);
					return fromBytes(bytes, 0, valueLen);
				} else {
					return null;
				}
			}
		}
		return null;
	}

	private BinaryString keyValueSlow(
		byte split1,
		byte split2,
		BinaryString keyName) {
		// position in byte
		int byteIdx = 0;
		// position of last split1
		int lastSplit1Idx = -1;
		while (byteIdx < sizeInBytes) {
			// If find next split1 in str, process current kv
			if (getByte(byteIdx) == split1) {
				int currentKeyIdx = lastSplit1Idx + 1;
				BinaryString value = findValueOfKeySlow(split2, keyName, currentKeyIdx, byteIdx);
				if (value != null) {
					return value;
				}
				lastSplit1Idx = byteIdx;
			}
			byteIdx++;
		}
		int currentKeyIdx = lastSplit1Idx + 1;
		return findValueOfKeySlow(split2, keyName, currentKeyIdx, sizeInBytes);
	}

	private BinaryString findValueOfKeySlow(
		byte split,
		BinaryString keyName,
		int start,
		int end) {
		int keyNameLen = keyName.sizeInBytes;
		for (int idx = start; idx < end; idx++) {
			if (getByte(idx) == split) {
				if (idx == start + keyNameLen &&
					SegmentsUtil.equals(segments, offset + start, keyName.segments,
						keyName.offset, keyNameLen)) {
					int valueIdx = idx + 1;
					byte[] bytes = SegmentsUtil.copyToBytes(segments, offset + valueIdx, end - valueIdx);
					return fromBytes(bytes);
				} else {
					return null;
				}
			}
		}
		return null;
	}

	/**
	 * Returns the position of the first occurence of substr in  current string starting from given
	 * position.
	 *
	 * @param subStr subStr to be searched
	 * @param start  start position
	 * @return the position of the first occurence of substring. Return -1 if not found.
	 */
	public int indexOf(BinaryString subStr, int start) {
		ensureMaterialized();
		subStr.ensureMaterialized();
		if (subStr.sizeInBytes == 0) {
			return 0;
		}
		if (inFirstSegment()) {
			// position in byte
			int byteIdx = 0;
			// position is char
			int charIdx = 0;
			while (byteIdx < sizeInBytes && charIdx < start) {
				byteIdx += numBytesForFirstByte(getByteOneSegment(byteIdx));
				charIdx++;
			}
			do {
				if (byteIdx + subStr.sizeInBytes > sizeInBytes) {
					return -1;
				}
				if (SegmentsUtil.equals(segments, offset + byteIdx,
					subStr.segments, subStr.offset, subStr.sizeInBytes)) {
					return charIdx;
				}
				byteIdx += numBytesForFirstByte(getByteOneSegment(byteIdx));
				charIdx++;
			} while (byteIdx < sizeInBytes);

			return -1;
		} else {
			return indexOfSlow(subStr, start);
		}
	}

	private int indexOfSlow(BinaryString subStr, int start) {
		// position in byte
		int byteIdx = 0;
		// position is char
		int charIdx = 0;
		int segSize = segments[0].size();
		SegmentAndOffset index = firstSegmentAndOffset(segSize);
		while (byteIdx < sizeInBytes && charIdx < start) {
			int charBytes = numBytesForFirstByte(index.value());
			byteIdx += charBytes;
			charIdx++;
			index.skipBytes(charBytes, segSize);
		}
		do {
			if (byteIdx + subStr.sizeInBytes > sizeInBytes) {
				return -1;
			}
			if (SegmentsUtil.equals(segments, offset + byteIdx,
				subStr.segments, subStr.offset, subStr.sizeInBytes)) {
				return charIdx;
			}
			int charBytes = numBytesForFirstByte(index.segment.get(index.offset));
			byteIdx += charBytes;
			charIdx++;
			index.skipBytes(charBytes, segSize);
		} while (byteIdx < sizeInBytes);

		return -1;
	}

	/**
	 * Reverse each character in current string.
	 *
	 * @return a new string which character order is reverse to current string.
	 */
	public BinaryString reverse() {
		ensureMaterialized();
		if (inFirstSegment()) {
			byte[] result = new byte[this.sizeInBytes];
			// position in byte
			int byteIdx = 0;
			while (byteIdx < sizeInBytes) {
				int charBytes = numBytesForFirstByte(getByteOneSegment(byteIdx));
				segments[0].get(
					offset + byteIdx,
					result,
					result.length - byteIdx - charBytes,
					charBytes);
				byteIdx += charBytes;
			}
			return BinaryString.fromBytes(result);
		} else {
			return reverseSlow();
		}
	}

	private BinaryString reverseSlow() {
		byte[] result = new byte[this.sizeInBytes];
		// position in byte
		int byteIdx = 0;
		int segSize = segments[0].size();
		SegmentAndOffset index = firstSegmentAndOffset(segSize);
		while (byteIdx < sizeInBytes) {
			int charBytes = numBytesForFirstByte(index.value());
			SegmentsUtil.copyMultiSegmentsToBytes(
				segments,
				offset + byteIdx,
				result,
				result.length - byteIdx - charBytes,
				charBytes);
			byteIdx += charBytes;
			index.skipBytes(charBytes, segSize);
		}
		return BinaryString.fromBytes(result);
	}


	// TODO repeat find rfind rpad lpad split
	// TODO upper/lower is slow?..

	private SegmentAndOffset firstSegmentAndOffset(int segSize) {
		int segIndex = offset / segSize;
		return new SegmentAndOffset(segIndex, offset % segSize);
	}

	private SegmentAndOffset lastSegmentAndOffset(int segSize) {
		int lastOffset = offset + sizeInBytes - 1;
		int segIndex = lastOffset / segSize;
		return new SegmentAndOffset(segIndex, lastOffset % segSize);
	}

	private SegmentAndOffset startSegmentAndOffset(int segSize) {
		if (inFirstSegment()) {
			return new SegmentAndOffset(0, offset);
		}
		else {
			return firstSegmentAndOffset(segSize);
		}
	}

	/**
	 * CurrentSegment and positionInSegment.
	 */
	private class SegmentAndOffset {
		int segIndex;
		MemorySegment segment;
		int offset;

		private SegmentAndOffset(int segIndex, int offset) {
			this.segIndex = segIndex;
			this.segment = segments[segIndex];
			this.offset = offset;
		}

		private void assignSegment() {
			if (segIndex >= 0 && segIndex < segments.length) {
				segment = segments[segIndex];
			} else {
				segment = null;
			}
		}

		private void previousByte(int segSize) {
			offset--;
			if (offset == -1) {
				segIndex--;
				assignSegment();
				offset = segSize - 1;
			}
		}

		private void nextByte(int segSize) {
			offset++;
			checkAdvance(segSize);
		}

		private void checkAdvance(int segSize) {
			if (offset == segSize) {
				advance();
			}
		}

		private void advance() {
			segIndex++;
			assignSegment();
			offset = 0;
		}

		private void skipBytes(int n, int segSize) {
			int remaining = segSize - this.offset;
			if (remaining > n) {
				this.offset += n;
			} else {
				while (true) {
					int toSkip = Math.min(remaining, n);
					n -= toSkip;
					if (n <= 0) {
						this.offset += toSkip;
						checkAdvance(segSize);
						return;
					}
					advance();
					remaining = segSize - this.offset;
				}
			}
		}

		private byte value() {
			return this.segment.get(this.offset);
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
	public Long toLong() {
		ensureMaterialized();
		if (sizeInBytes == 0) {
			return null;
		}
		int size = segments[0].size();
		SegmentAndOffset segmentAndOffset = startSegmentAndOffset(size);
		int totalOffset = 0;

		byte b = segmentAndOffset.value();
		final boolean negative = b == '-';
		if (negative || b == '+') {
			segmentAndOffset.nextByte(size);
			totalOffset++;
			if (sizeInBytes == 1) {
				return null;
			}
		}

		long result = 0;
		final byte separator = '.';
		final int radix = 10;
		final long stopValue = Long.MIN_VALUE / radix;
		while (totalOffset < this.sizeInBytes) {
			b = segmentAndOffset.value();
			totalOffset++;
			segmentAndOffset.nextByte(size);
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
		while (totalOffset < sizeInBytes) {
			byte currentByte = segmentAndOffset.value();
			if (currentByte < '0' || currentByte > '9') {
				return null;
			}
			totalOffset++;
			segmentAndOffset.nextByte(size);
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
	public Integer toInt() {
		ensureMaterialized();
		if (sizeInBytes == 0) {
			return null;
		}
		int size = segments[0].size();
		SegmentAndOffset segmentAndOffset = startSegmentAndOffset(size);
		int totalOffset = 0;

		byte b = segmentAndOffset.value();
		final boolean negative = b == '-';
		if (negative || b == '+') {
			segmentAndOffset.nextByte(size);
			totalOffset++;
			if (sizeInBytes == 1) {
				return null;
			}
		}

		int result = 0;
		final byte separator = '.';
		final int radix = 10;
		final long stopValue = Integer.MIN_VALUE / radix;
		while (totalOffset < this.sizeInBytes) {
			b = segmentAndOffset.value();
			totalOffset++;
			segmentAndOffset.nextByte(size);
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
		while (totalOffset < sizeInBytes) {
			byte currentByte = segmentAndOffset.value();
			if (currentByte < '0' || currentByte > '9') {
				return null;
			}
			totalOffset++;
			segmentAndOffset.nextByte(size);
		}

		if (!negative) {
			result = -result;
			if (result < 0) {
				return null;
			}
		}
		return result;
	}

	public Short toShort() {
		Integer intValue = toInt();
		if (intValue != null) {
			short result = intValue.shortValue();
			if (result == intValue) {
				return result;
			}
		}
		return null;
	}

	public Byte toByte() {
		Integer intValue = toInt();
		if (intValue != null) {
			byte result = intValue.byteValue();
			if (result == intValue) {
				return result;
			}
		}
		return null;
	}

	public Double toDouble() {
		try {
			return Double.valueOf(toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	public Float toFloat() {
		try {
			return Float.valueOf(toString());
		} catch (NumberFormatException e) {
			return null;
		}
	}

	/**
	 * Parses this BinaryString to Decimal.
	 *
	 * @return Decimal value if the parsing was successful, or null if overflow
	 * @throws NumberFormatException if the parsing failed.
	 */
	public Decimal toDecimal(int precision, int scale) {
		ensureMaterialized();
		if (precision > Decimal.MAX_LONG_DIGITS || this.sizeInBytes > Decimal.MAX_LONG_DIGITS) {
			return toDecimalSlow(precision, scale);
		}

		// Data in Decimal is stored by one long value if `precision` <= Decimal.MAX_LONG_DIGITS.
		// In this case we can directly extract the value from memory segment.
		int size = getSegments()[0].size();
		SegmentAndOffset segmentAndOffset = startSegmentAndOffset(size);
		int totalOffset = 0;

		// Remove white spaces at the beginning
		byte b = 0;
		while (totalOffset < this.sizeInBytes) {
			b = segmentAndOffset.value();
			if (b != ' ' && b != '\n' && b != '\t') {
				break;
			}
			totalOffset++;
			segmentAndOffset.nextByte(size);
		}
		if (totalOffset == this.sizeInBytes) {
			// all whitespaces
			return null;
		}

		// ======= Significand part begin =======
		final boolean negative = b == '-';
		if (negative || b == '+') {
			segmentAndOffset.nextByte(size);
			totalOffset++;
			if (totalOffset == this.sizeInBytes) {
				// only contains prefix plus/minus
				return null;
			}
		}

		long significand = 0;
		int exp = 0;
		int significandLen = 0, pointPos = -1;

		while (totalOffset < this.sizeInBytes) {
			b = segmentAndOffset.value();
			totalOffset++;
			segmentAndOffset.nextByte(size);

			if (b >= '0' && b <= '9') {
				// No need to worry about overflow, because this.sizeInBytes <= Decimal.MAX_LONG_DIGITS
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
		// ======= Significand part end =======

		// ======= Exponential part begin =======
		if ((b == 'e' || b == 'E') && totalOffset < this.sizeInBytes) {
			b = segmentAndOffset.value();
			final boolean expNegative = b == '-';
			if (expNegative || b == '+') {
				segmentAndOffset.nextByte(size);
				totalOffset++;
				if (totalOffset == this.sizeInBytes) {
					return null;
				}
			}

			int expDigits = 0;
			// As `precision` <= 18, value absolute range is limited to 10^-18 ~ 10^18.
			// The worst case is <18-digits>E-36
			final int expStopValue = 40;

			while (totalOffset < this.sizeInBytes) {
				b = segmentAndOffset.value();
				totalOffset++;
				segmentAndOffset.nextByte(size);

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
		// ======= Exponential part end =======

		// Check for invalid character at the end
		while (totalOffset < this.sizeInBytes) {
			b = segmentAndOffset.value();
			totalOffset++;
			segmentAndOffset.nextByte(size);
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

	private Decimal toDecimalSlow(int precision, int scale) {
		// As data in Decimal is currently stored by BigDecimal if `precision` > Decimal.MAX_LONG_DIGITS,
		// and BigDecimal only supports String or char[] for its constructor,
		// we can't directly extract the value from BinaryString.
		//
		// As BigDecimal(char[], int, int) is faster than BigDecimal(String, int, int),
		// we extract char[] from the memory segment and pass it to the constructor of BigDecimal.
		char[] chars = SegmentsUtil.allocateReuseChars(sizeInBytes);
		int len;
		if (segments.length == 1) {
			len = StringUtf8Utils.decodeUTF8Strict(segments[0], offset, sizeInBytes, chars);
		} else {
			byte[] bytes = SegmentsUtil.allocateReuseBytes(sizeInBytes);
			ensureMaterialized();
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
	 * Returns the upper case of this string.
	 */
	public BinaryString toUpperCase() {
		if (javaObject != null) {
			return toUpperCaseSlow();
		}
		if (sizeInBytes == 0) {
			return EMPTY_UTF8;
		}
		int size = segments[0].size();
		SegmentAndOffset segmentAndOffset = startSegmentAndOffset(size);
		byte[] bytes = new byte[sizeInBytes];
		bytes[0] = (byte) Character.toTitleCase(segmentAndOffset.value());
		for (int i = 0; i < sizeInBytes; i++) {
			byte b = segmentAndOffset.value();
			if (numBytesForFirstByte(b) != 1) {
				// fallback
				return toUpperCaseSlow();
			}
			int upper = Character.toUpperCase((int) b);
			if (upper > 127) {
				// fallback
				return toUpperCaseSlow();
			}
			bytes[i] = (byte) upper;
			segmentAndOffset.nextByte(size);
		}
		return fromBytes(bytes);
	}

	private BinaryString toUpperCaseSlow() {
		return fromString(toString().toUpperCase());
	}

	/**
	 * Returns the lower case of this string.
	 */
	public BinaryString toLowerCase() {
		if (javaObject != null) {
			return toLowerCaseSlow();
		}
		if (sizeInBytes == 0) {
			return EMPTY_UTF8;
		}
		int size = segments[0].size();
		SegmentAndOffset segmentAndOffset = startSegmentAndOffset(size);
		byte[] bytes = new byte[sizeInBytes];
		bytes[0] = (byte) Character.toTitleCase(segmentAndOffset.value());
		for (int i = 0; i < sizeInBytes; i++) {
			byte b = segmentAndOffset.value();
			if (numBytesForFirstByte(b) != 1) {
				// fallback
				return toLowerCaseSlow();
			}
			int lower = Character.toLowerCase((int) b);
			if (lower > 127) {
				// fallback
				return toLowerCaseSlow();
			}
			bytes[i] = (byte) lower;
			segmentAndOffset.nextByte(size);
		}
		return fromBytes(bytes);
	}

	private BinaryString toLowerCaseSlow() {
		return fromString(toString().toLowerCase());
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
	 * <p>Note: return BinaryStrings is reuse MemorySegments from this.</p>
	 *
	 * @param separator  String containing the String to be used as a delimiter,
	 *  {@code null} splits on whitespace
	 * @return an array of parsed Strings, {@code null} if null String was input
	 * @since 2.4
	 */
	public BinaryString[] splitByWholeSeparatorPreserveAllTokens(BinaryString separator) {
		ensureMaterialized();
		final int len = sizeInBytes;

		if (len == 0) {
			return EMPTY_STRING_ARRAY;
		}

		if (separator == null || EMPTY_UTF8.equals(separator)) {
			// Split on whitespace.
			return splitByWholeSeparatorPreserveAllTokens(fromString(" "));
		}
		separator.ensureMaterialized();

		final int separatorLength = separator.sizeInBytes;

		final ArrayList<BinaryString> substrings = new ArrayList<>();
		int beg = 0;
		int end = 0;
		while (end < len) {
			end = SegmentsUtil.find(
				segments, offset + beg, sizeInBytes - beg,
				separator.segments, separator.offset, separator.sizeInBytes) - offset;

			if (end > -1) {
				if (end > beg) {

					// The following is OK, because String.substring( beg, end ) excludes
					// the character at the position 'end'.
					substrings.add(BinaryString.fromAddress(segments, offset + beg, end - beg));

					// Set the starting point for the next search.
					// The following is equivalent to beg = end + (separatorLength - 1) + 1,
					// which is the right calculation:
					beg = end + separatorLength;
				} else {
					// We found a consecutive occurrence of the separator.
					substrings.add(EMPTY_UTF8);
					beg = end + separatorLength;
				}
			} else {
				// String.substring( beg ) goes from 'beg' to the end of the String.
				substrings.add(BinaryString.fromAddress(segments, offset + beg, sizeInBytes - beg));
				end = len;
			}
		}

		return substrings.toArray(new BinaryString[0]);
	}

	/**
	 * Calculate the hash value of a given string use {@link MessageDigest}.
	 */
	public BinaryString hash(MessageDigest md) {
		String str = EncodingUtils.hex(md.digest(getBytes()));
		return fromString(str);
	}

	public BinaryString hash(String algorithm) throws NoSuchAlgorithmException {
		return hash(MessageDigest.getInstance(algorithm));
	}

	private static final List<BinaryString> TRUE_STRINGS =
		Stream
			.of("t", "true", "y", "yes", "1")
			.map(BinaryString::fromString)
			.peek(BinaryString::ensureMaterialized)
			.collect(Collectors.toList());

	private static final List<BinaryString> FALSE_STRINGS =
		Stream
			.of("f", "false", "n", "no", "0")
			.map(BinaryString::fromString)
			.peek(BinaryString::ensureMaterialized)
			.collect(Collectors.toList());

	/**
	 * Decide boolean representation of a string.
	 */
	public Boolean toBooleanSQL() {
		if (TRUE_STRINGS.contains(toLowerCase())) {
			return true;
		} else if (FALSE_STRINGS.contains(toLowerCase())) {
			return false;
		} else {
			return null;
		}
	}

}
