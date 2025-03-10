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

package org.apache.flink.table.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Wrapper for a number of memory segments.
 * It appears to the user that it has a continuous segment of memory.
 * However, it dispatches the read/write requests to the underlying memory segments underneath.
 */
public abstract class MemorySegmentContainer {

	private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

	/**
	 * The size for each memory segment.
	 */
	protected final int segmentSize;

	/**
	 * Total size for all segments.
	 */
	protected final int totalSize;

	protected final MemorySegment[] segments;

	public static MemorySegmentContainer createMemorySegmentContainer(MemorySegment[] segments) {
		Preconditions.checkArgument(segments != null && segments.length >= 1,
			"The set of underlying memory segments cannot be empty");
		if (segments.length == 1) {
			return new SingleSegmentContainer(segments);
		} else {
			int segSize = segments[0].size();
			if ((segSize & (segSize - 1)) == 0) {
				return new Power2SegmentContainer(segments);
			} else {
				return new GeneralSegmentContainer(segments);
			}
		}
	}

	protected MemorySegmentContainer(MemorySegment[] segments) {
		this.segments = segments;
		segmentSize = segments[0].size();

		totalSize = segmentSize * segments.length;
	}

	protected abstract int getSegmentIndex(int index);

	protected abstract int getOffsetInSegment(int index);

	/**
	 * Expand this container with additional memory segments.
	 * @param additionalSegments new memory segments to add.
	 * @return the expanded memory segment.
	 */
	public MemorySegmentContainer expand(MemorySegment[] additionalSegments) {
		Preconditions.checkArgument(additionalSegments != null && additionalSegments.length >= 1,
			"The set of new memory segments cannot be empty");

		Preconditions.checkArgument(additionalSegments[0].size() == segmentSize,
			"The new segments must have the same segment size as the current ones.");

		MemorySegment[] newSegments = new MemorySegment[segments.length + additionalSegments.length];
		for (int i = 0; i < segments.length; i++) {
			newSegments[i] = segments[i];
		}

		for (int i = 0; i < additionalSegments.length; i++) {
			newSegments[i + segments.length] = additionalSegments[i];
		}

		return MemorySegmentContainer.createMemorySegmentContainer(newSegments);
	}

	public byte get(int index) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		return segments[segmentIndex].get(offsetInSegment);
	}

	public void put(int index, byte b) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		segments[segmentIndex].put(offsetInSegment, b);
	}

	public void get(int index, byte[] dst) {
		get(index, dst, 0, dst.length);
	}

	public void put(int index, byte[] src) {
		put(index, src, 0, src.length);
	}

	public void get(int index, byte[] dst, int offset, int length) {
		int srcIndex = index;
		int dstIndex = offset;
		int remainingCount = length;
		while (remainingCount > 0) {
			int segmentIndex = getSegmentIndex(srcIndex);
			int offsetInSegment = getOffsetInSegment(srcIndex);

			int remainingInSeg = segmentSize - offsetInSegment;
			if (remainingInSeg == 0) {
				segmentIndex += 1;
				offsetInSegment = 0;
				remainingInSeg = segmentSize;
			}

			int bytesToRead = remainingInSeg < remainingCount ? remainingInSeg : remainingCount;
			segments[segmentIndex].get(offsetInSegment, dst, dstIndex, bytesToRead);

			srcIndex += bytesToRead;
			remainingCount -= bytesToRead;
			dstIndex += bytesToRead;
		}
	}

	public void put(int index, byte[] src, int offset, int length) {
		int srcIndex = offset;
		int dstIndex = index;
		int remainingCount = length;
		while (remainingCount > 0) {
			int segmentIndex = getSegmentIndex(dstIndex);
			int offsetInSegment = getOffsetInSegment(dstIndex);

			int remainingInSeg = segmentSize - offsetInSegment;
			if (remainingInSeg == 0) {
				segmentIndex += 1;
				offsetInSegment = 0;
				remainingInSeg = segmentSize;
			}

			int bytesToWrite = remainingInSeg < remainingCount ? remainingInSeg : remainingCount;
			segments[segmentIndex].put(offsetInSegment, src, srcIndex, bytesToWrite);

			srcIndex += bytesToWrite;
			remainingCount -= bytesToWrite;
			dstIndex += bytesToWrite;
		}
	}

	public boolean getBoolean(int index) {
		return get(index) != 0;
	}

	public void putBoolean(int index, boolean value) {
		put(index, (byte) (value ? 1 : 0));
	}

	public void get(DataOutput out, int offset, int length) throws IOException {
		int index = offset;
		int remaining = length;
		while (remaining >= 8) {
			out.writeLong(getLongBigEndian(index));
			index += 8;
			remaining -= 8;
		}

		while (remaining > 0) {
			out.writeByte(get(index));
			index += 1;
			remaining -= 1;
		}
	}

	public final long getLongBigEndian(int index) {
		if (LITTLE_ENDIAN) {
			return Long.reverseBytes(getLong(index));
		} else {
			return getLong(index);
		}
	}

	public void put(DataInput in, int offset, int length) throws IOException {
		int index = offset;
		int remaining = length;

		while (remaining >= 8) {
			putLongBigEndian(index, in.readLong());
			index += 8;
			remaining -= 8;
		}

		while (remaining > 0) {
			put(index, in.readByte());
			index += 1;
			remaining -= 1;
		}
	}

	public final void putLongBigEndian(int index, long value) {
		if (LITTLE_ENDIAN) {
			putLong(index, Long.reverseBytes(value));
		} else {
			putLong(index, value);
		}
	}

	public void get(int offset, ByteBuffer target, int numBytes) {
		// check the byte array offset and length
		if ((offset | numBytes | (offset + numBytes)) < 0) {
			throw new IndexOutOfBoundsException();
		}

		int srcIndex = offset;
		int remainingCount = numBytes;
		while (remainingCount > 0) {
			int segmentIndex = getSegmentIndex(srcIndex);
			int offsetInSegment = getOffsetInSegment(srcIndex);

			int remainingInSeg = segmentSize - offsetInSegment;
			if (remainingInSeg == 0) {
				segmentIndex += 1;
				offsetInSegment = 0;
				remainingInSeg = segmentSize;
			}

			int bytesToRead = remainingInSeg < remainingCount ? remainingInSeg : remainingCount;
			segments[segmentIndex].get(offsetInSegment, target, bytesToRead);

			srcIndex += bytesToRead;
			remainingCount -= bytesToRead;
		}
	}

	public void put(int offset, ByteBuffer source, int numBytes) {
		if ((offset | numBytes | (offset + numBytes)) < 0) {
			throw new IndexOutOfBoundsException();
		}
		if (offset + numBytes > totalSize) {
			throw new IndexOutOfBoundsException();
		}

		int dstIndex = offset;
		int remainingCount = numBytes;
		while (remainingCount > 0) {
			int segmentIndex = getSegmentIndex(dstIndex);
			int offsetInSegment = getOffsetInSegment(dstIndex);

			int remainingInSeg = segmentSize - offsetInSegment;
			if (remainingInSeg == 0) {
				segmentIndex += 1;
				offsetInSegment = 0;
				remainingInSeg = segmentSize;
			}

			int bytesToWrite = remainingInSeg < remainingCount ? remainingInSeg : remainingCount;
			segments[segmentIndex].put(offsetInSegment, source, bytesToWrite);

			remainingCount -= bytesToWrite;
			dstIndex += bytesToWrite;
		}
	}

	public int size() {
		return totalSize;
	}

	public void free() {
		for (int i = 0; i < segments.length; i++) {
			segments[i].free();
		}
	}

	public boolean isOffHeap() {
		return segments[0].isOffHeap();
	}

	public char getChar(int index) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		int segmentRemaining = segmentSize - offsetInSegment;

		if (segmentRemaining >= 2) {
			return segments[segmentIndex].getChar(offsetInSegment);
		}

		// boundary case
		char ch = 0;
		if (LITTLE_ENDIAN) {
			byte b = get(index + 1);
			ch = (char) (b & 0x000000FF);
			b = get(index);
			ch <<= 8;
			ch = (char) (ch | (b & 0x000000FF));
		} else {
			byte b = get(index);
			ch = (char) (b & 0x000000FF);
			b = get(index + 1);
			ch <<= 8;
			ch = (char) (ch | (b & 0x000000FF));
		}
		return ch;
	}

	public void putChar(int index, char value) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		int segmentRemaining = segmentSize - offsetInSegment;

		if (segmentRemaining >= 2) {
			segments[segmentIndex].putChar(offsetInSegment, value);
			return;
		}

		// boundary case
		if (LITTLE_ENDIAN) {
			byte b = (byte) (value & 0x000000FF);
			put(index, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index + 1, b);
		} else {
			byte b = (byte) (value & 0x000000FF);
			put(index + 1, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index, b);
		}
	}

	public short getShort(int index) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		int segmentRemaining = segmentSize - offsetInSegment;

		if (segmentRemaining >= 2) {
			return segments[segmentIndex].getShort(offsetInSegment);
		}

		// boundary case
		short ret = 0;
		if (LITTLE_ENDIAN) {
			byte b = get(index + 1);
			ret = (short) (b & 0x000000FF);
			b = get(index);
			ret <<= 8;
			ret = (short) (ret | (b & 0x000000FF));
		} else {
			byte b = get(index);
			ret = (short) (b & 0x000000FF);
			b = get(index + 1);
			ret <<= 8;
			ret = (short) (ret | (b & 0x000000FF));
		}
		return ret;
	}

	public void putShort(int index, short value) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		int segmentRemaining = segmentSize - offsetInSegment;

		if (segmentRemaining >= 2) {
			segments[segmentIndex].putShort(offsetInSegment, value);
			return;
		}

		// boundary case
		if (LITTLE_ENDIAN) {
			byte b = (byte) (value & 0x000000FF);
			put(index, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index + 1, b);
		} else {
			byte b = (byte) (value & 0x000000FF);
			put(index + 1, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index, b);
		}
	}

	public int getInt(int index) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		int segmentRemaining = segmentSize - offsetInSegment;

		if (segmentRemaining >= 4) {
			return segments[segmentIndex].getInt(offsetInSegment);
		}

		// boundary case
		int ret = 0;
		if (LITTLE_ENDIAN) {
			byte b = get(index + 3);
			ret = b & 0x000000FF;
			b = get(index + 2);
			ret <<= 8;
			ret |= b & 0x000000FF;
			b = get(index + 1);
			ret <<= 8;
			ret |= b & 0x000000FF;
			b = get(index);
			ret <<= 8;
			ret |= b & 0x000000FF;
		} else {
			byte b = get(index);
			ret = b & 0x000000FF;
			b = get(index + 1);
			ret <<= 8;
			ret |= b & 0x000000FF;
			b = get(index + 2);
			ret <<= 8;
			ret |= b & 0x000000FF;
			b = get(index + 3);
			ret <<= 8;
			ret |= b & 0x000000FF;
		}
		return ret;
	}

	public void putInt(int index, int value) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		int segmentRemaining = segmentSize - offsetInSegment;

		if (segmentRemaining >= 4) {
			segments[segmentIndex].putInt(offsetInSegment, value);
			return;
		}

		// boundary case
		if (LITTLE_ENDIAN) {
			byte b = (byte) (value & 0x000000FF);
			put(index, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index + 1, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index + 2, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index + 3, b);
		} else {
			byte b = (byte) (value & 0x000000FF);
			put(index + 3, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index + 2, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index + 1, b);
			value >>>= 8;
			b = (byte) (value & 0x000000FF);
			put(index, b);
		}
	}

	public long getLong(int index) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		int segmentRemaining = segmentSize - offsetInSegment;

		if (segmentRemaining >= 8) {
			return segments[segmentIndex].getLong(offsetInSegment);
		}

		// boundary case
		long ret = 0;
		if (LITTLE_ENDIAN) {
			byte b = get(index + 7);
			ret = (long) b & 0x00000000000000FF;
			b = get(index + 6);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 5);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 4);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 3);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 2);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 1);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
		} else {
			byte b = get(index);
			ret = (long) b & 0x00000000000000FF;
			b = get(index + 1);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 2);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 3);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 4);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 5);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 6);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
			b = get(index + 7);
			ret <<= 8;
			ret |= b & 0x00000000000000FF;
		}
		return ret;
	}

	public void putLong(int index, long value) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		int segmentRemaining = segmentSize - offsetInSegment;

		if (segmentRemaining >= 8) {
			segments[segmentIndex].putLong(offsetInSegment, value);
			return;
		}

		// boundary case
		if (LITTLE_ENDIAN) {
			byte b = (byte) (value & 0x00000000000000FF);
			put(index, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 1, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 2, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 3, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 4, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 5, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 6, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 7, b);
		} else {
			byte b = (byte) (value & 0x00000000000000FF);
			put(index + 7, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 6, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 5, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 4, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 3, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 2, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index + 1, b);
			value >>>= 8;
			b = (byte) (value & 0x00000000000000FF);
			put(index, b);
		}
	}

	public boolean isFreed() {
		return segments[0].isFreed();
	}
}
