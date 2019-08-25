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
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfoFactory;
import org.apache.flink.table.runtime.util.SegmentsUtil;
import org.apache.flink.table.runtime.util.StringUtf8Utils;

import javax.annotation.Nonnull;

import java.util.Arrays;

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

	public BinaryString() {}

	private BinaryString(MemorySegment[] segments, int offset, int sizeInBytes) {
		super(segments, offset, sizeInBytes);
	}

	private BinaryString(String javaObject) {
		super(javaObject);
	}

	private BinaryString(MemorySegment[] segments, int offset, int sizeInBytes, String javaObject) {
		super(segments, offset, sizeInBytes, javaObject);
	}

	// ------------------------------------------------------------------------------------------
	// Constructor helper
	// ------------------------------------------------------------------------------------------

	/**
	 * Creates an BinaryString from given address (base and offset) and length.
	 */
	public static BinaryString fromAddress(
			MemorySegment[] segments, int offset, int numBytes) {
		return new BinaryString(segments, offset, numBytes);
	}

	/**
	 * Creates an BinaryString from given java String.
	 */
	public static BinaryString fromString(String str) {
		if (str == null) {
			return null;
		} else {
			return new BinaryString(str);
		}
	}

	/**
	 * Creates an BinaryString from given UTF-8 bytes.
	 */
	public static BinaryString fromBytes(byte[] bytes) {
		return fromBytes(bytes, 0, bytes.length);
	}

	/**
	 * Creates an BinaryString from given UTF-8 bytes with offset and number of bytes.
	 */
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

	// ------------------------------------------------------------------------------------------
	// Public methods on BinaryString
	// ------------------------------------------------------------------------------------------

	/**
	 * Returns the number of UTF-8 code points in the string.
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
			return numCharsMultiSegs();
		}
	}

	private int numCharsMultiSegs() {
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

	/**
	 * Returns the {@code byte} value at the specified index. An index ranges from {@code 0} to
	 * {@code getSizeInBytes() - 1}.
	 *
	 * @param      index   the index of the {@code byte} value.
	 * @return     the {@code byte} value at the specified index of this UTF-8 bytes.
	 * @exception  IndexOutOfBoundsException  if the {@code index}
	 *             argument is negative or not less than the length of this
	 *             UTF-8 bytes.
	 */
	public byte byteAt(int index) {
		ensureMaterialized();
		int globalOffset = offset + index;
		int size = segments[0].size();
		if (globalOffset < size) {
			return segments[0].get(globalOffset);
		} else {
			return segments[globalOffset / size].get(globalOffset % size);
		}
	}

	/**
	 * Get the underlying UTF-8 byte array, the returned bytes may be reused.
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

	/**
	 * Copy a new {@code BinaryString}.
	 */
	public BinaryString copy() {
		ensureMaterialized();
		byte[] copy = SegmentsUtil.copyToBytes(segments, offset, sizeInBytes);
		return new BinaryString(new MemorySegment[] {MemorySegmentFactory.wrap(copy)},
				0, sizeInBytes, javaObject);
	}

	/**
	 * Compares two strings lexicographically.
	 * Since UTF-8 uses groups of six bits, it is sometimes useful to use octal notation which
	 * uses 3-bit groups. With a calculator which can convert between hexadecimal and octal it
	 * can be easier to manually create or interpret UTF-8 compared with using binary.
	 * So we just compare the binary.
	 */
	@Override
	public int compareTo(@Nonnull BinaryString other) {
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

	/**
	 * Returns a binary string that is a substring of this binary string. The substring begins at
	 * the specified {@code beginIndex} and extends to the character at index {@code endIndex - 1}.
	 *
	 * <p>Examples:
	 * <blockquote><pre>
	 * fromString("hamburger").substring(4, 8) returns binary string "urge"
	 * fromString("smiles").substring(1, 5) returns binary string "mile"
	 * </pre></blockquote>
	 *
	 * @param beginIndex   the beginning index, inclusive.
	 * @param endIndex     the ending index, exclusive.
	 * @return the specified substring, return EMPTY_UTF8 when index out of bounds
	 * instead of StringIndexOutOfBoundsException.
	 */
	public BinaryString substring(int beginIndex, int endIndex) {
		ensureMaterialized();
		if (endIndex <= beginIndex || beginIndex >= sizeInBytes) {
			return EMPTY_UTF8;
		}
		if (inFirstSegment()) {
			MemorySegment segment = segments[0];
			int i = 0;
			int c = 0;
			while (i < sizeInBytes && c < beginIndex) {
				i += numBytesForFirstByte(segment.get(i + offset));
				c += 1;
			}

			int j = i;
			while (i < sizeInBytes && c < endIndex) {
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
			return substringMultiSegs(beginIndex, endIndex);
		}
	}

	private BinaryString substringMultiSegs(final int start, final int until) {
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
	 * Returns true if and only if this BinaryString contains the specified
	 * sequence of bytes values.
	 *
	 * @param s the sequence to search for
	 * @return true if this BinaryString contains {@code s}, false otherwise
	 */
	public boolean contains(final BinaryString s) {
		ensureMaterialized();
		s.ensureMaterialized();
		if (s.sizeInBytes == 0) {
			return true;
		}
		int find = SegmentsUtil.find(
			segments, offset, sizeInBytes,
			s.segments, s.offset, s.sizeInBytes);
		return find != -1;
	}

	/**
	 * Tests if this BinaryString starts with the specified prefix.
	 *
	 * @param   prefix   the prefix.
	 * @return  {@code true} if the bytes represented by the argument is a prefix of the bytes
	 *          represented by this string; {@code false} otherwise. Note also that {@code true}
	 *          will be returned if the argument is an empty BinaryString or is equal to this
	 *          {@code BinaryString} object as determined by the {@link #equals(Object)} method.
	 */
	public boolean startsWith(final BinaryString prefix) {
		ensureMaterialized();
		prefix.ensureMaterialized();
		return matchAt(prefix, 0);
	}

	/**
	 * Tests if this BinaryString ends with the specified suffix.
	 *
	 * @param   suffix   the suffix.
	 * @return  {@code true} if the bytes represented by the argument is a suffix of the bytes
	 *          represented by this object; {@code false} otherwise. Note that the result will
	 *          be {@code true} if the argument is the empty string or is equal to this
	 *          {@code BinaryString} object as determined by the {@link #equals(Object)} method.
	 */
	public boolean endsWith(final BinaryString suffix) {
		ensureMaterialized();
		suffix.ensureMaterialized();
		return matchAt(suffix, sizeInBytes - suffix.sizeInBytes);
	}

	/**
	 * Returns a string whose value is this string, with any leading and trailing
	 * whitespace removed.
	 *
	 * @return  A string whose value is this string, with any leading and trailing white
	 *          space removed, or this string if it has no leading or
	 *          trailing white space.
	 */
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
				return copyBinaryStringInOneSeg(s, e - s + 1);
			}
		} else {
			return trimMultiSegs();
		}
	}

	private BinaryString trimMultiSegs() {
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
	 * Returns the index within this string of the first occurrence of the
	 * specified substring, starting at the specified index.
	 *
	 * @param   str         the substring to search for.
	 * @param   fromIndex   the index from which to start the search.
	 * @return  the index of the first occurrence of the specified substring,
	 *          starting at the specified index,
	 *          or {@code -1} if there is no such occurrence.
	 */
	public int indexOf(BinaryString str, int fromIndex) {
		ensureMaterialized();
		str.ensureMaterialized();
		if (str.sizeInBytes == 0) {
			return 0;
		}
		if (inFirstSegment()) {
			// position in byte
			int byteIdx = 0;
			// position is char
			int charIdx = 0;
			while (byteIdx < sizeInBytes && charIdx < fromIndex) {
				byteIdx += numBytesForFirstByte(getByteOneSegment(byteIdx));
				charIdx++;
			}
			do {
				if (byteIdx + str.sizeInBytes > sizeInBytes) {
					return -1;
				}
				if (SegmentsUtil.equals(segments, offset + byteIdx,
						str.segments, str.offset, str.sizeInBytes)) {
					return charIdx;
				}
				byteIdx += numBytesForFirstByte(getByteOneSegment(byteIdx));
				charIdx++;
			} while (byteIdx < sizeInBytes);

			return -1;
		} else {
			return indexOfMultiSegs(str, fromIndex);
		}
	}

	private int indexOfMultiSegs(BinaryString str, int fromIndex) {
		// position in byte
		int byteIdx = 0;
		// position is char
		int charIdx = 0;
		int segSize = segments[0].size();
		SegmentAndOffset index = firstSegmentAndOffset(segSize);
		while (byteIdx < sizeInBytes && charIdx < fromIndex) {
			int charBytes = numBytesForFirstByte(index.value());
			byteIdx += charBytes;
			charIdx++;
			index.skipBytes(charBytes, segSize);
		}
		do {
			if (byteIdx + str.sizeInBytes > sizeInBytes) {
				return -1;
			}
			if (SegmentsUtil.equals(segments, offset + byteIdx,
					str.segments, str.offset, str.sizeInBytes)) {
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
	 * Converts all of the characters in this {@code BinaryString} to upper case.
	 *
	 * @return the {@code BinaryString}, converted to uppercase.
	 */
	public BinaryString toUpperCase() {
		if (javaObject != null) {
			return javaToUpperCase();
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
				return javaToUpperCase();
			}
			int upper = Character.toUpperCase((int) b);
			if (upper > 127) {
				// fallback
				return javaToUpperCase();
			}
			bytes[i] = (byte) upper;
			segmentAndOffset.nextByte(size);
		}
		return fromBytes(bytes);
	}

	private BinaryString javaToUpperCase() {
		return fromString(toString().toUpperCase());
	}

	/**
	 * Converts all of the characters in this {@code BinaryString} to lower case.
	 *
	 * @return the {@code BinaryString}, converted to lowercase.
	 */
	public BinaryString toLowerCase() {
		if (javaObject != null) {
			return javaToLowerCase();
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
				return javaToLowerCase();
			}
			int lower = Character.toLowerCase((int) b);
			if (lower > 127) {
				// fallback
				return javaToLowerCase();
			}
			bytes[i] = (byte) lower;
			segmentAndOffset.nextByte(size);
		}
		return fromBytes(bytes);
	}

	private BinaryString javaToLowerCase() {
		return fromString(toString().toLowerCase());
	}

	// ------------------------------------------------------------------------------------------
	// Internal methods on BinaryString
	// ------------------------------------------------------------------------------------------

	byte getByteOneSegment(int i) {
		return segments[0].get(offset + i);
	}

	boolean inFirstSegment() {
		return sizeInBytes + offset <= segments[0].size();
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

	BinaryString copyBinaryStringInOneSeg(int start, int len) {
		byte[] newBytes = new byte[len];
		segments[0].get(offset + start, newBytes, 0, len);
		return fromBytes(newBytes);
	}

	BinaryString copyBinaryString(int start, int end) {
		int len = end - start + 1;
		byte[] newBytes = new byte[len];
		SegmentsUtil.copyToBytes(segments, offset + start, newBytes, 0, len);
		return fromBytes(newBytes);
	}

	SegmentAndOffset firstSegmentAndOffset(int segSize) {
		int segIndex = offset / segSize;
		return new SegmentAndOffset(segIndex, offset % segSize);
	}

	SegmentAndOffset lastSegmentAndOffset(int segSize) {
		int lastOffset = offset + sizeInBytes - 1;
		int segIndex = lastOffset / segSize;
		return new SegmentAndOffset(segIndex, lastOffset % segSize);
	}

	private SegmentAndOffset startSegmentAndOffset(int segSize) {
		return inFirstSegment() ? new SegmentAndOffset(0, offset) : firstSegmentAndOffset(segSize);
	}

	/**
	 * CurrentSegment and positionInSegment.
	 */
	class SegmentAndOffset {
		int segIndex;
		MemorySegment segment;
		int offset;

		private SegmentAndOffset(int segIndex, int offset) {
			this.segIndex = segIndex;
			this.segment = segments[segIndex];
			this.offset = offset;
		}

		private void assignSegment() {
			segment = segIndex >= 0 && segIndex < segments.length ?
					segments[segIndex] : null;
		}

		void previousByte(int segSize) {
			offset--;
			if (offset == -1) {
				segIndex--;
				assignSegment();
				offset = segSize - 1;
			}
		}

		void nextByte(int segSize) {
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

		void skipBytes(int n, int segSize) {
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

		byte value() {
			return this.segment.get(this.offset);
		}
	}

	/**
	 * Returns the number of bytes for a code point with the first byte as `b`.
	 * @param b The first byte of a code point
	 */
	static int numBytesForFirstByte(final byte b) {
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
			// Skip the first byte disallowed in UTF-8
			// Handling errors quietly, same semantics to java String.
			return 1;
		}
	}
}
