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

package org.apache.flink.core.memory;

import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A memory segment based on a number of underlying memory segment.
 * It appears to the user that it has a continuous segment of memory.
 * However, it dispatches the read/write requests to the underlying memory segments underneath.
 * This class will greatly simplify many operations & algorithms, which invest large amounts of code
 * dealing with boundary cases.
 */
public class ContainerMemorySegment extends MemorySegment {

	protected final int segmentSize;

	protected final int segmentSizeMask;

	protected final int segmentSizeBitCount;

	protected final int totalSize;

	protected final MemorySegment[] segments;

	ContainerMemorySegment(Object owner, MemorySegment[] segments) {
		super(owner);
		Preconditions.checkArgument(segments != null && segments.length >= 2,
			"The set of underlying memory segments for cannot be empty");
		this.segments = segments;
		segmentSize = segments[0].size;

		Preconditions.checkArgument((segmentSize & (segmentSize - 1)) == 0,
			"The segment size must be a power of 2.");

		segmentSizeMask = segmentSize - 1;
		segmentSizeBitCount = MathUtils.log2strict(segmentSize);

		totalSize = segmentSize * segments.length;
	}

	@Override
	public ByteBuffer wrap(int offset, int length) {
		throw new UnsupportedOperationException("ContainerMemorySegment does not support the wrap method");
	}

	private int getSegmentIndex(int index) {
		return index >>> segmentSizeBitCount;
	}

	private int getOffsetInSegment(int index) {
		return index & segmentSizeMask;
	}

	@Override
	public byte get(int index) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		return segments[segmentIndex].get(offsetInSegment);
	}

	@Override
	public void put(int index, byte b) {
		int segmentIndex = getSegmentIndex(index);
		int offsetInSegment = getOffsetInSegment(index);
		segments[segmentIndex].put(offsetInSegment, b);
	}

	@Override
	public void get(int index, byte[] dst) {
		get(index, dst, 0, dst.length);
	}

	@Override
	public void put(int index, byte[] src) {
		put(index, src, 0, src.length);
	}

	@Override
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

	@Override
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

	@Override
	public boolean getBoolean(int index) {
		return get(index) != 0;
	}

	@Override
	public void putBoolean(int index, boolean value) {
		put(index, (byte) (value ? 1 : 0));
	}

	@Override
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

	@Override
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

	@Override
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

	@Override
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

	@Override
	public int size() {
		return totalSize;
	}

	@Override
	public void free() {
		for (int i = 0; i < segments.length; i++) {
			segments[i].free();
		}
	}

	@Override
	public boolean isOffHeap() {
		return segments[0].isOffHeap();
	}

	@Override
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

	@Override
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

	@Override
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

	@Override
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

	@Override
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

	@Override
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

	@Override
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

	@Override
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

	@Override
	public void swapBytes(byte[] tempBuffer, MemorySegment seg2, int offset1, int offset2, int len) {
		this.get(offset1, tempBuffer, 0, len);

		// TODO: improve the performance for this
		byte[] tempBuffer2 = new byte[len];
		seg2.get(offset2, tempBuffer2, 0, len);

		this.put(offset1, tempBuffer2, 0, len);
		seg2.put(offset2, tempBuffer, 0, len);
	}

	@Override
	public boolean isFreed() {
		return segments[0].isFreed();
	}
}
