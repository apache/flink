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

package org.apache.flink.table.runtime.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.nio.ByteOrder;

import static org.apache.flink.core.memory.MemoryUtils.UNSAFE;

/**
 * Util for data format segments calc.
 */
public class SegmentsUtil {

	/**
	 * Constant that flags the byte order.
	 */
	public static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

	private static final int ADDRESS_BITS_PER_WORD = 3;

	private static final int BIT_BYTE_INDEX_MASK = 7;

	/**
	 * SQL execution threads is limited, not too many, so it can bear the overhead of 64K per thread.
	 */
	private static final int MAX_BYTES_LENGTH = 1024 * 64;
	private static final int MAX_CHARS_LENGTH = 1024 * 32;

	private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

	private static final ThreadLocal<byte[]> BYTES_LOCAL = new ThreadLocal<>();
	private static final ThreadLocal<char[]> CHARS_LOCAL = new ThreadLocal<>();

	/**
	 * Allocate bytes that is only for temporary usage, it should not be stored in somewhere else.
	 * Use a {@link ThreadLocal} to reuse bytes to avoid overhead of byte[] new and gc.
	 *
	 * <p>If there are methods that can only accept a byte[], instead of a MemorySegment[]
	 * parameter, we can allocate a reuse bytes and copy the MemorySegment data to byte[],
	 * then call the method. Such as String deserialization.
	 */
	public static byte[] allocateReuseBytes(int length) {
		byte[] bytes = BYTES_LOCAL.get();

		if (bytes == null) {
			if (length <= MAX_BYTES_LENGTH) {
				bytes = new byte[MAX_BYTES_LENGTH];
				BYTES_LOCAL.set(bytes);
			} else {
				bytes = new byte[length];
			}
		} else if (bytes.length < length) {
			bytes = new byte[length];
		}

		return bytes;
	}

	public static char[] allocateReuseChars(int length) {
		char[] chars = CHARS_LOCAL.get();

		if (chars == null) {
			if (length <= MAX_CHARS_LENGTH) {
				chars = new char[MAX_CHARS_LENGTH];
				CHARS_LOCAL.set(chars);
			} else {
				chars = new char[length];
			}
		} else if (chars.length < length) {
			chars = new char[length];
		}

		return chars;
	}



	/**
	 * Copy segments to a new byte[].
	 *
	 * @param segments Source segments.
	 * @param offset Source segments offset.
	 * @param numBytes the number bytes to copy.
	 */
	public static byte[] copyToBytes(MemorySegment[] segments, int offset, int numBytes) {
		return copyToBytes(segments, offset, new byte[numBytes], 0, numBytes);
	}

	/**
	 * Copy segments to target byte[].
	 *
	 * @param segments Source segments.
	 * @param offset Source segments offset.
	 * @param bytes target byte[].
	 * @param bytesOffset target byte[] offset.
	 * @param numBytes the number bytes to copy.
	 */
	public static byte[] copyToBytes(MemorySegment[] segments, int offset, byte[] bytes,
			int bytesOffset, int numBytes) {
		if (inFirstSegment(segments, offset, numBytes)) {
			segments[0].get(offset, bytes, bytesOffset, numBytes);
		} else {
			copyMultiSegmentsToBytes(segments, offset, bytes, bytesOffset, numBytes);
		}
		return bytes;
	}

	public static void copyMultiSegmentsToBytes(MemorySegment[] segments, int offset, byte[] bytes,
			int bytesOffset, int numBytes) {
		int remainSize = numBytes;
		for (MemorySegment segment : segments) {
			int remain = segment.size() - offset;
			if (remain > 0) {
				int nCopy = Math.min(remain, remainSize);
				segment.get(offset, bytes, numBytes - remainSize + bytesOffset, nCopy);
				remainSize -= nCopy;
				// next new segment.
				offset = 0;
				if (remainSize == 0) {
					return;
				}
			} else {
				// remain is negative, let's advance to next segment
				// now the offset = offset - segmentSize (-remain)
				offset = -remain;
			}
		}
	}

	/**
	 * Copy segments to target unsafe pointer.
	 *
	 * @param segments Source segments.
	 * @param offset   The position where the bytes are started to be read from these memory
	 *                 segments.
	 * @param target   The unsafe memory to copy the bytes to.
	 * @param pointer  The position in the target unsafe memory to copy the chunk to.
	 * @param numBytes the number bytes to copy.
	 */
	public static void copyToUnsafe(
			MemorySegment[] segments,
			int offset,
			Object target,
			int pointer,
			int numBytes) {
		if (inFirstSegment(segments, offset, numBytes)) {
			segments[0].copyToUnsafe(offset, target, pointer, numBytes);
		} else {
			copyMultiSegmentsToUnsafe(segments, offset, target, pointer, numBytes);
		}
	}

	private static void copyMultiSegmentsToUnsafe(
			MemorySegment[] segments,
			int offset,
			Object target,
			int pointer,
			int numBytes) {
		int remainSize = numBytes;
		for (MemorySegment segment : segments) {
			int remain = segment.size() - offset;
			if (remain > 0) {
				int nCopy = Math.min(remain, remainSize);
				segment.copyToUnsafe(offset, target, numBytes - remainSize + pointer, nCopy);
				remainSize -= nCopy;
				// next new segment.
				offset = 0;
				if (remainSize == 0) {
					return;
				}
			} else {
				// remain is negative, let's advance to next segment
				// now the offset = offset - segmentSize (-remain)
				offset = -remain;
			}
		}
	}


	/**
	 * Copy bytes of segments to output view.
	 * Note: It just copies the data in, not include the length.
	 *
	 * @param segments    source segments
	 * @param offset      offset for segments
	 * @param sizeInBytes size in bytes
	 * @param target      target output view
	 */
	public static void copyToView(
			MemorySegment[] segments,
			int offset,
			int sizeInBytes,
			DataOutputView target) throws IOException {
		for (MemorySegment sourceSegment : segments) {
			int curSegRemain = sourceSegment.size() - offset;
			if (curSegRemain > 0) {
				int copySize = Math.min(curSegRemain, sizeInBytes);

				byte[] bytes = allocateReuseBytes(copySize);
				sourceSegment.get(offset, bytes, 0, copySize);
				target.write(bytes, 0, copySize);

				sizeInBytes -= copySize;
				offset = 0;
			} else {
				offset -= sourceSegment.size();
			}

			if (sizeInBytes == 0) {
				return;
			}
		}

		if (sizeInBytes != 0) {
			throw new RuntimeException("No copy finished, this should be a bug, " +
					"The remaining length is: " + sizeInBytes);
		}
	}

	/**
	 * Copy target segments from source byte[].
	 *
	 * @param segments target segments.
	 * @param offset target segments offset.
	 * @param bytes source byte[].
	 * @param bytesOffset source byte[] offset.
	 * @param numBytes the number bytes to copy.
	 */
	public static void copyFromBytes(
			MemorySegment[] segments, int offset,
			byte[] bytes, int bytesOffset, int numBytes) {
		if (segments.length == 1) {
			segments[0].put(offset, bytes, bytesOffset, numBytes);
		} else {
			copyMultiSegmentsFromBytes(segments, offset, bytes, bytesOffset, numBytes);
		}
	}

	private static void copyMultiSegmentsFromBytes(
			MemorySegment[] segments, int offset, byte[] bytes, int bytesOffset, int numBytes) {
		int remainSize = numBytes;
		for (MemorySegment segment : segments) {
			int remain = segment.size() - offset;
			if (remain > 0) {
				int nCopy = Math.min(remain, remainSize);
				segment.put(offset, bytes, numBytes - remainSize + bytesOffset, nCopy);
				remainSize -= nCopy;
				// next new segment.
				offset = 0;
				if (remainSize == 0) {
					return;
				}
			} else {
				// remain is negative, let's advance to next segment
				// now the offset = offset - segmentSize (-remain)
				offset = -remain;
			}
		}
	}

	/**
	 * Maybe not copied, if want copy, please use copyTo.
	 */
	public static byte[] getBytes(MemorySegment[] segments, int baseOffset, int sizeInBytes) {
		// avoid copy if `base` is `byte[]`
		if (segments.length == 1) {
			byte[] heapMemory = segments[0].getHeapMemory();
			if (baseOffset == 0
				&& heapMemory != null
				&& heapMemory.length == sizeInBytes) {
				return heapMemory;
			} else {
				byte[] bytes = new byte[sizeInBytes];
				segments[0].get(baseOffset, bytes, 0, sizeInBytes);
				return bytes;
			}
		} else {
			byte[] bytes = new byte[sizeInBytes];
			copyMultiSegmentsToBytes(segments, baseOffset, bytes, 0, sizeInBytes);
			return bytes;
		}
	}

	/**
	 * Equals two memory segments regions.
	 *
	 * @param segments1 Segments 1
	 * @param offset1 Offset of segments1 to start equaling
	 * @param segments2 Segments 2
	 * @param offset2 Offset of segments2 to start equaling
	 * @param len Length of the equaled memory region
	 *
	 * @return true if equal, false otherwise
	 */
	public static boolean equals(
			MemorySegment[] segments1, int offset1,
			MemorySegment[] segments2, int offset2, int len) {
		if (inFirstSegment(segments1, offset1, len) && inFirstSegment(segments2, offset2, len)) {
			return segments1[0].equalTo(segments2[0], offset1, offset2, len);
		} else {
			return equalsMultiSegments(segments1, offset1, segments2, offset2, len);
		}
	}

	@VisibleForTesting
	static boolean equalsMultiSegments(
			MemorySegment[] segments1, int offset1,
			MemorySegment[] segments2, int offset2, int len) {
		if (len == 0) {
			// quick way and avoid segSize is zero.
			return true;
		}

		int segSize1 = segments1[0].size();
		int segSize2 = segments2[0].size();

		// find first segIndex and segOffset of segments.
		int segIndex1 = offset1 / segSize1;
		int segIndex2 = offset2 / segSize2;
		int segOffset1 = offset1 - segSize1 * segIndex1; // equal to %
		int segOffset2 = offset2 - segSize2 * segIndex2; // equal to %

		while (len > 0) {
			int equalLen = Math.min(Math.min(len, segSize1 - segOffset1), segSize2 - segOffset2);
			if (!segments1[segIndex1].equalTo(segments2[segIndex2], segOffset1, segOffset2, equalLen)) {
				return false;
			}
			len -= equalLen;
			segOffset1 += equalLen;
			if (segOffset1 == segSize1) {
				segOffset1 = 0;
				segIndex1++;
			}
			segOffset2 += equalLen;
			if (segOffset2 == segSize2) {
				segOffset2 = 0;
				segIndex2++;
			}
		}
		return true;
	}

	/**
	 * hash segments to int, numBytes must be aligned to 4 bytes.
	 *
	 * @param segments Source segments.
	 * @param offset Source segments offset.
	 * @param numBytes the number bytes to hash.
	 */
	public static int hashByWords(MemorySegment[] segments, int offset, int numBytes) {
		if (inFirstSegment(segments, offset, numBytes)) {
			return MurmurHashUtil.hashBytesByWords(segments[0], offset, numBytes);
		} else {
			return hashMultiSegByWords(segments, offset, numBytes);
		}
	}

	private static int hashMultiSegByWords(MemorySegment[] segments, int offset, int numBytes) {
		byte[] bytes = allocateReuseBytes(numBytes);
		copyMultiSegmentsToBytes(segments, offset, bytes, 0, numBytes);
		return MurmurHashUtil.hashUnsafeBytesByWords(bytes, BYTE_ARRAY_BASE_OFFSET, numBytes);
	}

	/**
	 * hash segments to int.
	 *
	 * @param segments Source segments.
	 * @param offset Source segments offset.
	 * @param numBytes the number bytes to hash.
	 */
	public static int hash(MemorySegment[] segments, int offset, int numBytes) {
		if (inFirstSegment(segments, offset, numBytes)) {
			return MurmurHashUtil.hashBytes(segments[0], offset, numBytes);
		} else {
			return hashMultiSeg(segments, offset, numBytes);
		}
	}

	private static int hashMultiSeg(MemorySegment[] segments, int offset, int numBytes) {
		byte[] bytes = allocateReuseBytes(numBytes);
		copyMultiSegmentsToBytes(segments, offset, bytes, 0, numBytes);
		return MurmurHashUtil.hashUnsafeBytes(bytes, BYTE_ARRAY_BASE_OFFSET, numBytes);
	}

	/**
	 * Is it just in first MemorySegment, we use quick way to do something.
	 */
	private static boolean inFirstSegment(MemorySegment[] segments, int offset, int numBytes) {
		return numBytes + offset <= segments[0].size();
	}

	/**
	 * Given a bit index, return the byte index containing it.
	 * @param bitIndex the bit index.
	 * @return the byte index.
	 */
	private static int byteIndex(int bitIndex) {
		return bitIndex >>> ADDRESS_BITS_PER_WORD;
	}

	/**
	 * unset bit.
	 *
	 * @param segment target segment.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static void bitUnSet(MemorySegment segment, int baseOffset, int index) {
		int offset = baseOffset + byteIndex(index);
		byte current = segment.get(offset);
		current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(offset, current);
	}

	/**
	 * set bit.
	 *
	 * @param segment target segment.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static void bitSet(MemorySegment segment, int baseOffset, int index) {
		int offset = baseOffset + byteIndex(index);
		byte current = segment.get(offset);
		current |= (1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(offset, current);
	}

	/**
	 * read bit.
	 *
	 * @param segment target segment.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static boolean bitGet(MemorySegment segment, int baseOffset, int index) {
		int offset = baseOffset + byteIndex(index);
		byte current = segment.get(offset);
		return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
	}

	/**
	 * unset bit from segments.
	 *
	 * @param segments target segments.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static void bitUnSet(MemorySegment[] segments, int baseOffset, int index) {
		if (segments.length == 1) {
			MemorySegment segment = segments[0];
			int offset = baseOffset + byteIndex(index);
			byte current = segment.get(offset);
			current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
			segment.put(offset, current);
		} else {
			bitUnSetMultiSegments(segments, baseOffset, index);
		}
	}

	private static void bitUnSetMultiSegments(MemorySegment[] segments, int baseOffset, int index) {
		int offset = baseOffset + byteIndex(index);
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		MemorySegment segment = segments[segIndex];

		byte current = segment.get(segOffset);
		current &= ~(1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(segOffset, current);
	}

	/**
	 * set bit from segments.
	 *
	 * @param segments target segments.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static void bitSet(MemorySegment[] segments, int baseOffset, int index) {
		if (segments.length == 1) {
			int offset = baseOffset + byteIndex(index);
			MemorySegment segment = segments[0];
			byte current = segment.get(offset);
			current |= (1 << (index & BIT_BYTE_INDEX_MASK));
			segment.put(offset, current);
		} else {
			bitSetMultiSegments(segments, baseOffset, index);
		}
	}

	private static void bitSetMultiSegments(MemorySegment[] segments, int baseOffset, int index) {
		int offset = baseOffset + byteIndex(index);
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		MemorySegment segment = segments[segIndex];

		byte current = segment.get(segOffset);
		current |= (1 << (index & BIT_BYTE_INDEX_MASK));
		segment.put(segOffset, current);
	}

	/**
	 * read bit from segments.
	 *
	 * @param segments target segments.
	 * @param baseOffset bits base offset.
	 * @param index bit index from base offset.
	 */
	public static boolean bitGet(MemorySegment[] segments, int baseOffset, int index) {
		int offset = baseOffset + byteIndex(index);
		byte current = getByte(segments, offset);
		return (current & (1 << (index & BIT_BYTE_INDEX_MASK))) != 0;
	}

	/**
	 * get boolean from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static boolean getBoolean(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 1)) {
			return segments[0].getBoolean(offset);
		} else {
			return getBooleanMultiSegments(segments, offset);
		}
	}

	private static boolean getBooleanMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		return segments[segIndex].getBoolean(segOffset);
	}

	/**
	 * set boolean from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setBoolean(MemorySegment[] segments, int offset, boolean value) {
		if (inFirstSegment(segments, offset, 1)) {
			segments[0].putBoolean(offset, value);
		} else {
			setBooleanMultiSegments(segments, offset, value);
		}
	}

	private static void setBooleanMultiSegments(MemorySegment[] segments, int offset, boolean value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		segments[segIndex].putBoolean(segOffset, value);
	}

	/**
	 * get byte from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static byte getByte(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 1)) {
			return segments[0].get(offset);
		} else {
			return getByteMultiSegments(segments, offset);
		}
	}

	private static byte getByteMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		return segments[segIndex].get(segOffset);
	}

	/**
	 * set byte from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setByte(MemorySegment[] segments, int offset, byte value) {
		if (inFirstSegment(segments, offset, 1)) {
			segments[0].put(offset, value);
		} else {
			setByteMultiSegments(segments, offset, value);
		}
	}

	private static void setByteMultiSegments(MemorySegment[] segments, int offset, byte value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %
		segments[segIndex].put(segOffset, value);
	}

	/**
	 * get int from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static int getInt(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 4)) {
			return segments[0].getInt(offset);
		} else {
			return getIntMultiSegments(segments, offset);
		}
	}

	private static int getIntMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			return segments[segIndex].getInt(segOffset);
		} else {
			return getIntSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	private static int getIntSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset) {
		MemorySegment segment = segments[segNum];
		int ret = 0;
		for (int i = 0; i < 4; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			int unsignedByte = segment.get(segOffset) & 0xff;
			if (LITTLE_ENDIAN) {
				ret |= (unsignedByte << (i * 8));
			} else {
				ret |= (unsignedByte << ((3 - i) * 8));
			}
			segOffset++;
		}
		return ret;
	}

	/**
	 * set int from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setInt(MemorySegment[] segments, int offset, int value) {
		if (inFirstSegment(segments, offset, 4)) {
			segments[0].putInt(offset, value);
		} else {
			setIntMultiSegments(segments, offset, value);
		}
	}

	private static void setIntMultiSegments(MemorySegment[] segments, int offset, int value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			segments[segIndex].putInt(segOffset, value);
		} else {
			setIntSlowly(segments, segSize, segIndex, segOffset, value);
		}
	}

	private static void setIntSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset, int value) {
		MemorySegment segment = segments[segNum];
		for (int i = 0; i < 4; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			int unsignedByte;
			if (LITTLE_ENDIAN) {
				unsignedByte = value >> (i * 8);
			} else {
				unsignedByte = value >> ((3 - i) * 8);
			}
			segment.put(segOffset, (byte) unsignedByte);
			segOffset++;
		}
	}

	/**
	 * get long from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static long getLong(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 8)) {
			return segments[0].getLong(offset);
		} else {
			return getLongMultiSegments(segments, offset);
		}
	}

	private static long getLongMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			return segments[segIndex].getLong(segOffset);
		} else {
			return getLongSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	private static long getLongSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset) {
		MemorySegment segment = segments[segNum];
		long ret = 0;
		for (int i = 0; i < 8; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			long unsignedByte = segment.get(segOffset) & 0xff;
			if (LITTLE_ENDIAN) {
				ret |= (unsignedByte << (i * 8));
			} else {
				ret |= (unsignedByte << ((7 - i) * 8));
			}
			segOffset++;
		}
		return ret;
	}

	/**
	 * set long from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setLong(MemorySegment[] segments, int offset, long value) {
		if (inFirstSegment(segments, offset, 8)) {
			segments[0].putLong(offset, value);
		} else {
			setLongMultiSegments(segments, offset, value);
		}
	}

	private static void setLongMultiSegments(MemorySegment[] segments, int offset, long value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			segments[segIndex].putLong(segOffset, value);
		} else {
			setLongSlowly(segments, segSize, segIndex, segOffset, value);
		}
	}

	private static void setLongSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset, long value) {
		MemorySegment segment = segments[segNum];
		for (int i = 0; i < 8; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			long unsignedByte;
			if (LITTLE_ENDIAN) {
				unsignedByte = value >> (i * 8);
			} else {
				unsignedByte = value >> ((7 - i) * 8);
			}
			segment.put(segOffset, (byte) unsignedByte);
			segOffset++;
		}
	}

	/**
	 * get short from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static short getShort(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 2)) {
			return segments[0].getShort(offset);
		} else {
			return getShortMultiSegments(segments, offset);
		}
	}

	private static short getShortMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 1) {
			return segments[segIndex].getShort(segOffset);
		} else {
			return (short) getTwoByteSlowly(segments, segSize, segIndex, segOffset);
		}
	}

	/**
	 * set short from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setShort(MemorySegment[] segments, int offset, short value) {
		if (inFirstSegment(segments, offset, 2)) {
			segments[0].putShort(offset, value);
		} else {
			setShortMultiSegments(segments, offset, value);
		}
	}

	private static void setShortMultiSegments(MemorySegment[] segments, int offset, short value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 1) {
			segments[segIndex].putShort(segOffset, value);
		} else {
			setTwoByteSlowly(segments, segSize, segIndex, segOffset, value, value >> 8);
		}
	}

	/**
	 * get float from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static float getFloat(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 4)) {
			return segments[0].getFloat(offset);
		} else {
			return getFloatMultiSegments(segments, offset);
		}
	}

	private static float getFloatMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			return segments[segIndex].getFloat(segOffset);
		} else {
			return Float.intBitsToFloat(getIntSlowly(segments, segSize, segIndex, segOffset));
		}
	}

	/**
	 * set float from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setFloat(MemorySegment[] segments, int offset, float value) {
		if (inFirstSegment(segments, offset, 4)) {
			segments[0].putFloat(offset, value);
		} else {
			setFloatMultiSegments(segments, offset, value);
		}
	}

	private static void setFloatMultiSegments(MemorySegment[] segments, int offset, float value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 3) {
			segments[segIndex].putFloat(segOffset, value);
		} else {
			setIntSlowly(segments, segSize, segIndex, segOffset, Float.floatToRawIntBits(value));
		}
	}

	/**
	 * get double from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static double getDouble(MemorySegment[] segments, int offset) {
		if (inFirstSegment(segments, offset, 8)) {
			return segments[0].getDouble(offset);
		} else {
			return getDoubleMultiSegments(segments, offset);
		}
	}

	private static double getDoubleMultiSegments(MemorySegment[] segments, int offset) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			return segments[segIndex].getDouble(segOffset);
		} else {
			return Double.longBitsToDouble(getLongSlowly(segments, segSize, segIndex, segOffset));
		}
	}

	/**
	 * set double from segments.
	 *
	 * @param segments target segments.
	 * @param offset value offset.
	 */
	public static void setDouble(MemorySegment[] segments, int offset, double value) {
		if (inFirstSegment(segments, offset, 8)) {
			segments[0].putDouble(offset, value);
		} else {
			setDoubleMultiSegments(segments, offset, value);
		}
	}

	private static void setDoubleMultiSegments(MemorySegment[] segments, int offset, double value) {
		int segSize = segments[0].size();
		int segIndex = offset / segSize;
		int segOffset = offset - segIndex * segSize; // equal to %

		if (segOffset < segSize - 7) {
			segments[segIndex].putDouble(segOffset, value);
		} else {
			setLongSlowly(segments, segSize, segIndex, segOffset, Double.doubleToRawLongBits(value));
		}
	}

	private static int getTwoByteSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset) {
		MemorySegment segment = segments[segNum];
		int ret = 0;
		for (int i = 0; i < 2; i++) {
			if (segOffset == segSize) {
				segment = segments[++segNum];
				segOffset = 0;
			}
			int unsignedByte = segment.get(segOffset) & 0xff;
			if (LITTLE_ENDIAN) {
				ret |= (unsignedByte << (i * 8));
			} else {
				ret |= (unsignedByte << ((1 - i) * 8));
			}
			segOffset++;
		}
		return ret;
	}

	private static void setTwoByteSlowly(
			MemorySegment[] segments, int segSize, int segNum, int segOffset, int b1, int b2) {
		MemorySegment segment = segments[segNum];
		segment.put(segOffset, (byte) (LITTLE_ENDIAN ? b1 : b2));
		segOffset++;
		if (segOffset == segSize) {
			segment = segments[++segNum];
			segOffset = 0;
		}
		segment.put(segOffset, (byte) (LITTLE_ENDIAN ? b2 : b1));
	}

	/**
	 * Find equal segments2 in segments1.
	 * @param segments1 segs to find.
	 * @param segments2 sub segs.
	 * @return Return the found offset, return -1 if not find.
	 */
	public static int find(
		MemorySegment[] segments1, int offset1, int numBytes1,
		MemorySegment[] segments2, int offset2, int numBytes2) {
		if (numBytes2 == 0) { // quick way 1.
			return offset1;
		}
		if (inFirstSegment(segments1, offset1, numBytes1) &&
			inFirstSegment(segments2, offset2, numBytes2)) {
			byte first = segments2[0].get(offset2);
			int end = numBytes1 - numBytes2 + offset1;
			for (int i = offset1; i <= end; i++) {
				// quick way 2: equal first byte.
				if (segments1[0].get(i) == first &&
					segments1[0].equalTo(segments2[0], i, offset2, numBytes2)) {
					return i;
				}
			}
			return -1;
		} else {
			return findInMultiSegments(segments1, offset1, numBytes1, segments2, offset2, numBytes2);
		}
	}

	private static int findInMultiSegments(
		MemorySegment[] segments1, int offset1, int numBytes1,
		MemorySegment[] segments2, int offset2, int numBytes2) {
		int end = numBytes1 - numBytes2 + offset1;
		for (int i = offset1; i <= end; i++) {
			if (equalsMultiSegments(segments1, i, segments2, offset2, numBytes2)) {
				return i;
			}
		}
		return -1;
	}
}
