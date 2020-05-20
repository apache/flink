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

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.StringData;

import javax.annotation.Nonnull;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A lazily binary implementation of {@link StringData} which is backed by {@link MemorySegment}s
 * and {@link String}.
 *
 * <p>Either {@link MemorySegment}s or {@link String} must be provided when
 * constructing {@link BinaryStringData}. The other representation will be materialized when needed.
 *
 * <p>It provides many useful methods for comparison, search, and so on.
 */
@Internal
public final class BinaryStringData extends LazyBinaryFormat<String> implements StringData {

	public static final BinaryStringData EMPTY_UTF8 = BinaryStringData.fromBytes(StringUtf8Utils.encodeUTF8(""));

	public BinaryStringData() {}

	public BinaryStringData(String javaObject) {
		super(javaObject);
	}

	public BinaryStringData(MemorySegment[] segments, int offset, int sizeInBytes) {
		super(segments, offset, sizeInBytes);
	}

	public BinaryStringData(MemorySegment[] segments, int offset, int sizeInBytes, String javaObject) {
		super(segments, offset, sizeInBytes, javaObject);
	}

	// ------------------------------------------------------------------------------------------
	// Construction Utilities
	// ------------------------------------------------------------------------------------------

	/**
	 * Creates a {@link BinaryStringData} instance from the given address (base and offset) and length.
	 */
	public static BinaryStringData fromAddress(
			MemorySegment[] segments,
			int offset,
			int numBytes) {
		return new BinaryStringData(segments, offset, numBytes);
	}

	/**
	 * Creates a {@link BinaryStringData} instance from the given Java string.
	 */
	public static BinaryStringData fromString(String str) {
		if (str == null) {
			return null;
		} else {
			return new BinaryStringData(str);
		}
	}

	/**
	 * Creates a {@link BinaryStringData} instance from the given UTF-8 bytes.
	 */
	public static BinaryStringData fromBytes(byte[] bytes) {
		return fromBytes(bytes, 0, bytes.length);
	}

	/**
	 * Creates a {@link BinaryStringData} instance from the given UTF-8 bytes with offset and number of bytes.
	 */
	public static BinaryStringData fromBytes(byte[] bytes, int offset, int numBytes) {
		return new BinaryStringData(
			new MemorySegment[] {MemorySegmentFactory.wrap(bytes)},
			offset,
			numBytes);
	}

	/**
	 * Creates a {@link BinaryStringData} instance that contains `length` spaces.
	 */
	public static BinaryStringData blankString(int length) {
		byte[] spaces = new byte[length];
		Arrays.fill(spaces, (byte) ' ');
		return fromBytes(spaces);
	}

	// ------------------------------------------------------------------------------------------
	// Public Interfaces
	// ------------------------------------------------------------------------------------------

	@Override
	public byte[] toBytes() {
		ensureMaterialized();
		return BinarySegmentUtils.getBytes(
			binarySection.segments,
			binarySection.offset,
			binarySection.sizeInBytes);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BinaryStringData) {
			BinaryStringData other = (BinaryStringData) o;
			if (javaObject != null && other.javaObject != null) {
				return javaObject.equals(other.javaObject);
			}

			ensureMaterialized();
			other.ensureMaterialized();
			return binarySection.equals(other.binarySection);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		ensureMaterialized();
		return binarySection.hashCode();
	}

	@Override
	public String toString() {
		if (javaObject == null) {
			byte[] bytes = BinarySegmentUtils.allocateReuseBytes(binarySection.sizeInBytes);
			BinarySegmentUtils.copyToBytes(binarySection.segments, binarySection.offset, bytes, 0, binarySection.sizeInBytes);
			javaObject = StringUtf8Utils.decodeUTF8(bytes, 0, binarySection.sizeInBytes);
		}
		return javaObject;
	}

	/**
	 * Compares two strings lexicographically.
	 * Since UTF-8 uses groups of six bits, it is sometimes useful to use octal notation which
	 * uses 3-bit groups. With a calculator which can convert between hexadecimal and octal it
	 * can be easier to manually create or interpret UTF-8 compared with using binary.
	 * So we just compare the binary.
	 */
	@Override
	public int compareTo(@Nonnull StringData o) {
		// BinaryStringData is the only implementation of StringData
		BinaryStringData other = (BinaryStringData) o;
		if (javaObject != null && other.javaObject != null) {
			return javaObject.compareTo(other.javaObject);
		}

		ensureMaterialized();
		other.ensureMaterialized();
		if (binarySection.segments.length == 1 && other.binarySection.segments.length == 1) {

			int len = Math.min(binarySection.sizeInBytes, other.binarySection.sizeInBytes);
			MemorySegment seg1 = binarySection.segments[0];
			MemorySegment seg2 = other.binarySection.segments[0];

			for (int i = 0; i < len; i++) {
				int res =
					(seg1.get(binarySection.offset + i) & 0xFF) - (seg2.get(other.binarySection.offset + i) & 0xFF);
				if (res != 0) {
					return res;
				}
			}
			return binarySection.sizeInBytes - other.binarySection.sizeInBytes;
		}

		// if there are multi segments.
		return compareMultiSegments(other);
	}

	/**
	 * Find the boundaries of segments, and then compare MemorySegment.
	 */
	private int compareMultiSegments(BinaryStringData other) {

		if (binarySection.sizeInBytes == 0 || other.binarySection.sizeInBytes == 0) {
			return binarySection.sizeInBytes - other.binarySection.sizeInBytes;
		}

		int len = Math.min(binarySection.sizeInBytes, other.binarySection.sizeInBytes);

		MemorySegment seg1 = binarySection.segments[0];
		MemorySegment seg2 = other.binarySection.segments[0];

		int segmentSize = binarySection.segments[0].size();
		int otherSegmentSize = other.binarySection.segments[0].size();

		int sizeOfFirst1 = segmentSize - binarySection.offset;
		int sizeOfFirst2 = otherSegmentSize - other.binarySection.offset;

		int varSegIndex1 = 1;
		int varSegIndex2 = 1;

		// find the first segment of this string.
		while (sizeOfFirst1 <= 0) {
			sizeOfFirst1 += segmentSize;
			seg1 = binarySection.segments[varSegIndex1++];
		}

		while (sizeOfFirst2 <= 0) {
			sizeOfFirst2 += otherSegmentSize;
			seg2 = other.binarySection.segments[varSegIndex2++];
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
				seg1 = binarySection.segments[varSegIndex1++];
				offset1 = 0;
				offset2 += needCompare;
				sizeOfFirst1 = segmentSize;
				sizeOfFirst2 -= needCompare;
			} else if (sizeOfFirst1 > sizeOfFirst2) { //other is smaller
				seg2 = other.binarySection.segments[varSegIndex2++];
				offset2 = 0;
				offset1 += needCompare;
				sizeOfFirst2 = otherSegmentSize;
				sizeOfFirst1 -= needCompare;
			} else { // same, should go ahead both.
				seg1 = binarySection.segments[varSegIndex1++];
				seg2 = other.binarySection.segments[varSegIndex2++];
				offset1 = 0;
				offset2 = 0;
				sizeOfFirst1 = segmentSize;
				sizeOfFirst2 = otherSegmentSize;
			}
			needCompare = Math.min(Math.min(sizeOfFirst1, sizeOfFirst2), len);
		}

		checkArgument(needCompare == len);

		return binarySection.sizeInBytes - other.binarySection.sizeInBytes;
	}

	// ------------------------------------------------------------------------------------------
	// Public methods on BinaryStringData
	// ------------------------------------------------------------------------------------------

	/**
	 * Returns the number of UTF-8 code points in the string.
	 */
	public int numChars() {
		ensureMaterialized();
		if (inFirstSegment()) {
			int len = 0;
			for (int i = 0; i < binarySection.sizeInBytes; i += numBytesForFirstByte(getByteOneSegment(i))) {
				len++;
			}
			return len;
		} else {
			return numCharsMultiSegs();
		}
	}

	private int numCharsMultiSegs() {
		int len = 0;
		int segSize = binarySection.segments[0].size();
		BinaryStringData.SegmentAndOffset index = firstSegmentAndOffset(segSize);
		int i = 0;
		while (i < binarySection.sizeInBytes) {
			int charBytes = numBytesForFirstByte(index.value());
			i += charBytes;
			len++;
			index.skipBytes(charBytes, segSize);
		}
		return len;
	}

	/**
	 * Returns the {@code byte} value at the specified index. An index ranges from {@code 0} to
	 * {@code binarySection.sizeInBytes - 1}.
	 *
	 * @param      index   the index of the {@code byte} value.
	 * @return     the {@code byte} value at the specified index of this UTF-8 bytes.
	 * @exception  IndexOutOfBoundsException  if the {@code index}
	 *             argument is negative or not less than the length of this
	 *             UTF-8 bytes.
	 */
	public byte byteAt(int index) {
		ensureMaterialized();
		int globalOffset = binarySection.offset + index;
		int size = binarySection.segments[0].size();
		if (globalOffset < size) {
			return binarySection.segments[0].get(globalOffset);
		} else {
			return binarySection.segments[globalOffset / size].get(globalOffset % size);
		}
	}

	@Override
	public MemorySegment[] getSegments() {
		ensureMaterialized();
		return super.getSegments();
	}

	@Override
	public int getOffset() {
		ensureMaterialized();
		return super.getOffset();
	}

	@Override
	public int getSizeInBytes() {
		ensureMaterialized();
		return super.getSizeInBytes();
	}

	public void ensureMaterialized() {
		ensureMaterialized(null);
	}

	@Override
	protected BinarySection materialize(TypeSerializer<String> serializer) {
		if (serializer != null) {
			throw new IllegalArgumentException("BinaryStringData does not support custom serializers");
		}

		byte[] bytes = StringUtf8Utils.encodeUTF8(javaObject);
		return new BinarySection(
			new MemorySegment[]{MemorySegmentFactory.wrap(bytes)},
			0,
			bytes.length
		);
	}

	/**
	 * Copy a new {@code BinaryStringData}.
	 */
	public BinaryStringData copy() {
		ensureMaterialized();
		byte[] copy = BinarySegmentUtils.copyToBytes(binarySection.segments, binarySection.offset, binarySection.sizeInBytes);
		return new BinaryStringData(new MemorySegment[] {MemorySegmentFactory.wrap(copy)},
			0, binarySection.sizeInBytes, javaObject);
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
	public BinaryStringData substring(int beginIndex, int endIndex) {
		ensureMaterialized();
		if (endIndex <= beginIndex || beginIndex >= binarySection.sizeInBytes) {
			return EMPTY_UTF8;
		}
		if (inFirstSegment()) {
			MemorySegment segment = binarySection.segments[0];
			int i = 0;
			int c = 0;
			while (i < binarySection.sizeInBytes && c < beginIndex) {
				i += numBytesForFirstByte(segment.get(i + binarySection.offset));
				c += 1;
			}

			int j = i;
			while (i < binarySection.sizeInBytes && c < endIndex) {
				i += numBytesForFirstByte(segment.get(i + binarySection.offset));
				c += 1;
			}

			if (i > j) {
				byte[] bytes = new byte[i - j];
				segment.get(binarySection.offset + j, bytes, 0, i - j);
				return fromBytes(bytes);
			} else {
				return EMPTY_UTF8;
			}
		} else {
			return substringMultiSegs(beginIndex, endIndex);
		}
	}

	private BinaryStringData substringMultiSegs(final int start, final int until) {
		int segSize = binarySection.segments[0].size();
		BinaryStringData.SegmentAndOffset index = firstSegmentAndOffset(segSize);
		int i = 0;
		int c = 0;
		while (i < binarySection.sizeInBytes && c < start) {
			int charSize = numBytesForFirstByte(index.value());
			i += charSize;
			index.skipBytes(charSize, segSize);
			c += 1;
		}

		int j = i;
		while (i < binarySection.sizeInBytes && c < until) {
			int charSize = numBytesForFirstByte(index.value());
			i += charSize;
			index.skipBytes(charSize, segSize);
			c += 1;
		}

		if (i > j) {
			return fromBytes(BinarySegmentUtils.copyToBytes(binarySection.segments, binarySection.offset + j, i - j));
		} else {
			return EMPTY_UTF8;
		}
	}

	/**
	 * Returns true if and only if this BinaryStringData contains the specified
	 * sequence of bytes values.
	 *
	 * @param s the sequence to search for
	 * @return true if this BinaryStringData contains {@code s}, false otherwise
	 */
	public boolean contains(final BinaryStringData s) {
		ensureMaterialized();
		s.ensureMaterialized();
		if (s.binarySection.sizeInBytes == 0) {
			return true;
		}
		int find = BinarySegmentUtils.find(
			binarySection.segments, binarySection.offset, binarySection.sizeInBytes,
			s.binarySection.segments, s.binarySection.offset, s.binarySection.sizeInBytes);
		return find != -1;
	}

	/**
	 * Tests if this BinaryStringData starts with the specified prefix.
	 *
	 * @param   prefix   the prefix.
	 * @return  {@code true} if the bytes represented by the argument is a prefix of the bytes
	 *          represented by this string; {@code false} otherwise. Note also that {@code true}
	 *          will be returned if the argument is an empty BinaryStringData or is equal to this
	 *          {@code BinaryStringData} object as determined by the {@link #equals(Object)} method.
	 */
	public boolean startsWith(final BinaryStringData prefix) {
		ensureMaterialized();
		prefix.ensureMaterialized();
		return matchAt(prefix, 0);
	}

	/**
	 * Tests if this BinaryStringData ends with the specified suffix.
	 *
	 * @param   suffix   the suffix.
	 * @return  {@code true} if the bytes represented by the argument is a suffix of the bytes
	 *          represented by this object; {@code false} otherwise. Note that the result will
	 *          be {@code true} if the argument is the empty string or is equal to this
	 *          {@code BinaryStringData} object as determined by the {@link #equals(Object)} method.
	 */
	public boolean endsWith(final BinaryStringData suffix) {
		ensureMaterialized();
		suffix.ensureMaterialized();
		return matchAt(suffix, binarySection.sizeInBytes - suffix.binarySection.sizeInBytes);
	}

	/**
	 * Returns a string whose value is this string, with any leading and trailing
	 * whitespace removed.
	 *
	 * @return  A string whose value is this string, with any leading and trailing white
	 *          space removed, or this string if it has no leading or
	 *          trailing white space.
	 */
	public BinaryStringData trim() {
		ensureMaterialized();
		if (inFirstSegment()) {
			int s = 0;
			int e = this.binarySection.sizeInBytes - 1;
			// skip all of the space (0x20) in the left side
			while (s < this.binarySection.sizeInBytes && getByteOneSegment(s) == 0x20) {
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

	private BinaryStringData trimMultiSegs() {
		int s = 0;
		int e = this.binarySection.sizeInBytes - 1;
		int segSize = binarySection.segments[0].size();
		BinaryStringData.SegmentAndOffset front = firstSegmentAndOffset(segSize);
		// skip all of the space (0x20) in the left side
		while (s < this.binarySection.sizeInBytes && front.value() == 0x20) {
			s++;
			front.nextByte(segSize);
		}
		BinaryStringData.SegmentAndOffset behind = lastSegmentAndOffset(segSize);
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
	public int indexOf(BinaryStringData str, int fromIndex) {
		ensureMaterialized();
		str.ensureMaterialized();
		if (str.binarySection.sizeInBytes == 0) {
			return 0;
		}
		if (inFirstSegment()) {
			// position in byte
			int byteIdx = 0;
			// position is char
			int charIdx = 0;
			while (byteIdx < binarySection.sizeInBytes && charIdx < fromIndex) {
				byteIdx += numBytesForFirstByte(getByteOneSegment(byteIdx));
				charIdx++;
			}
			do {
				if (byteIdx + str.binarySection.sizeInBytes > binarySection.sizeInBytes) {
					return -1;
				}
				if (BinarySegmentUtils.equals(binarySection.segments, binarySection.offset + byteIdx,
					str.binarySection.segments, str.binarySection.offset, str.binarySection.sizeInBytes)) {
					return charIdx;
				}
				byteIdx += numBytesForFirstByte(getByteOneSegment(byteIdx));
				charIdx++;
			} while (byteIdx < binarySection.sizeInBytes);

			return -1;
		} else {
			return indexOfMultiSegs(str, fromIndex);
		}
	}

	private int indexOfMultiSegs(BinaryStringData str, int fromIndex) {
		// position in byte
		int byteIdx = 0;
		// position is char
		int charIdx = 0;
		int segSize = binarySection.segments[0].size();
		BinaryStringData.SegmentAndOffset index = firstSegmentAndOffset(segSize);
		while (byteIdx < binarySection.sizeInBytes && charIdx < fromIndex) {
			int charBytes = numBytesForFirstByte(index.value());
			byteIdx += charBytes;
			charIdx++;
			index.skipBytes(charBytes, segSize);
		}
		do {
			if (byteIdx + str.binarySection.sizeInBytes > binarySection.sizeInBytes) {
				return -1;
			}
			if (BinarySegmentUtils.equals(binarySection.segments, binarySection.offset + byteIdx,
				str.binarySection.segments, str.binarySection.offset, str.binarySection.sizeInBytes)) {
				return charIdx;
			}
			int charBytes = numBytesForFirstByte(index.segment.get(index.offset));
			byteIdx += charBytes;
			charIdx++;
			index.skipBytes(charBytes, segSize);
		} while (byteIdx < binarySection.sizeInBytes);

		return -1;
	}

	/**
	 * Converts all of the characters in this {@code BinaryStringData} to upper case.
	 *
	 * @return the {@code BinaryStringData}, converted to uppercase.
	 */
	public BinaryStringData toUpperCase() {
		if (javaObject != null) {
			return javaToUpperCase();
		}
		if (binarySection.sizeInBytes == 0) {
			return EMPTY_UTF8;
		}
		int size = binarySection.segments[0].size();
		BinaryStringData.SegmentAndOffset segmentAndOffset = startSegmentAndOffset(size);
		byte[] bytes = new byte[binarySection.sizeInBytes];
		bytes[0] = (byte) Character.toTitleCase(segmentAndOffset.value());
		for (int i = 0; i < binarySection.sizeInBytes; i++) {
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

	private BinaryStringData javaToUpperCase() {
		return fromString(toString().toUpperCase());
	}

	/**
	 * Converts all of the characters in this {@code BinaryStringData} to lower case.
	 *
	 * @return the {@code BinaryStringData}, converted to lowercase.
	 */
	public BinaryStringData toLowerCase() {
		if (javaObject != null) {
			return javaToLowerCase();
		}
		if (binarySection.sizeInBytes == 0) {
			return EMPTY_UTF8;
		}
		int size = binarySection.segments[0].size();
		BinaryStringData.SegmentAndOffset segmentAndOffset = startSegmentAndOffset(size);
		byte[] bytes = new byte[binarySection.sizeInBytes];
		bytes[0] = (byte) Character.toTitleCase(segmentAndOffset.value());
		for (int i = 0; i < binarySection.sizeInBytes; i++) {
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

	private BinaryStringData javaToLowerCase() {
		return fromString(toString().toLowerCase());
	}

	// ------------------------------------------------------------------------------------------
	// Internal methods on BinaryStringData
	// ------------------------------------------------------------------------------------------

	byte getByteOneSegment(int i) {
		return binarySection.segments[0].get(binarySection.offset + i);
	}

	boolean inFirstSegment() {
		return binarySection.sizeInBytes + binarySection.offset <= binarySection.segments[0].size();
	}

	private boolean matchAt(final BinaryStringData s, int pos) {
		return (inFirstSegment() && s.inFirstSegment()) ? matchAtOneSeg(s, pos) : matchAtVarSeg(s, pos);
	}

	private boolean matchAtOneSeg(final BinaryStringData s, int pos) {
		return s.binarySection.sizeInBytes + pos <= binarySection.sizeInBytes && pos >= 0 &&
			binarySection.segments[0].equalTo(
				s.binarySection.segments[0],
				binarySection.offset + pos,
				s.binarySection.offset,
				s.binarySection.sizeInBytes);
	}

	private boolean matchAtVarSeg(final BinaryStringData s, int pos) {
		return s.binarySection.sizeInBytes + pos <= binarySection.sizeInBytes && pos >= 0 &&
			BinarySegmentUtils.equals(
				binarySection.segments,
				binarySection.offset + pos,
				s.binarySection.segments,
				s.binarySection.offset,
				s.binarySection.sizeInBytes);
	}

	BinaryStringData copyBinaryStringInOneSeg(int start, int len) {
		byte[] newBytes = new byte[len];
		binarySection.segments[0].get(binarySection.offset + start, newBytes, 0, len);
		return fromBytes(newBytes);
	}

	BinaryStringData copyBinaryString(int start, int end) {
		int len = end - start + 1;
		byte[] newBytes = new byte[len];
		BinarySegmentUtils.copyToBytes(binarySection.segments, binarySection.offset + start, newBytes, 0, len);
		return fromBytes(newBytes);
	}

	BinaryStringData.SegmentAndOffset firstSegmentAndOffset(int segSize) {
		int segIndex = binarySection.offset / segSize;
		return new BinaryStringData.SegmentAndOffset(segIndex, binarySection.offset % segSize);
	}

	BinaryStringData.SegmentAndOffset lastSegmentAndOffset(int segSize) {
		int lastOffset = binarySection.offset + binarySection.sizeInBytes - 1;
		int segIndex = lastOffset / segSize;
		return new BinaryStringData.SegmentAndOffset(segIndex, lastOffset % segSize);
	}

	private BinaryStringData.SegmentAndOffset startSegmentAndOffset(int segSize) {
		return inFirstSegment() ? new BinaryStringData.SegmentAndOffset(0, binarySection.offset) : firstSegmentAndOffset(segSize);
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
			this.segment = binarySection.segments[segIndex];
			this.offset = offset;
		}

		private void assignSegment() {
			segment = segIndex >= 0 && segIndex < binarySection.segments.length ?
				binarySection.segments[segIndex] : null;
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
