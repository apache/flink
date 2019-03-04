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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.util.BinaryStringUtil;
import org.apache.flink.table.util.SegmentsUtil;

import java.util.Arrays;

/**
 * Use the special format to write data to a {@link MemorySegment} (its capacity grows
 * automatically).
 *
 * <p>If write a format binary:
 * 1. New a writer.
 * 2. Write each field by writeXX or setNullAt. (Variable length fields can not be written
 * repeatedly.)
 * 3. Invoke {@link #complete()}.
 *
 * <p>If want to reuse this writer, please invoke {@link #reset()} first.
 */
public abstract class AbstractBinaryWriter implements BinaryWriter {

	protected MemorySegment segment;

	protected int cursor;

	/**
	 * Set offset and size to fix len part.
	 */
	protected abstract void setOffsetAndSize(int pos, int offset, long size);

	/**
	 *  Get field offset.
	 */
	protected abstract int getFieldOffset(int pos);

	/**
	 * After grow, need point to new memory.
	 */
	protected abstract void afterGrow();

	/**
	 * See {@link BinaryString#readBinaryStringFieldFromSegments}.
	 */
	@Override
	public void writeString(int pos, BinaryString input) {
		int len = input.getSizeInBytes();
		if (len <= 7) {
			byte[] bytes = BinaryStringUtil.allocateReuseBytes(len);
			SegmentsUtil.copyToBytes(input.getSegments(), input.getOffset(), bytes, 0, len);
			writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
		} else {
			writeSegmentsToVarLenPart(pos, input.getSegments(), input.getOffset(), len);
		}
	}

	@Override
	public void writeArray(int pos, BinaryArray input) {
		writeSegmentsToVarLenPart(pos, input.getSegments(), input.getOffset(), input.getSizeInBytes());
	}

	@Override
	public void writeMap(int pos, BinaryMap input) {
		writeSegmentsToVarLenPart(pos, input.getSegments(), input.getOffset(), input.getSizeInBytes());
	}

	private void zeroOutPaddingBytes(int numBytes) {
		if ((numBytes & 0x07) > 0) {
			segment.putLong(cursor + ((numBytes >> 3) << 3), 0L);
		}
	}

	private void ensureCapacity(int neededSize) {
		final int length = cursor + neededSize;
		if (segment.size() < length) {
			grow(length);
		}
	}

	private void writeSegmentsToVarLenPart(int pos, MemorySegment[] segments, int offset, int size) {
		final int roundedSize = roundNumberOfBytesToNearestWord(size);

		// grow the global buffer before writing data.
		ensureCapacity(roundedSize);

		zeroOutPaddingBytes(size);

		if (segments.length == 1) {
			segments[0].copyTo(offset, segment, cursor, size);
		} else {
			writeMultiSegmentsToVarLenPart(segments, offset, size);
		}

		setOffsetAndSize(pos, cursor, size);

		// move the cursor forward.
		cursor += roundedSize;
	}

	private void writeMultiSegmentsToVarLenPart(MemorySegment[] segments, int offset, int size) {
		// Write the bytes to the variable length portion.
		int needCopy = size;
		int fromOffset = offset;
		int toOffset = cursor;
		for (MemorySegment sourceSegment : segments) {
			int remain = sourceSegment.size() - fromOffset;
			if (remain > 0) {
				int copySize = remain > needCopy ? needCopy : remain;
				sourceSegment.copyTo(fromOffset, segment, toOffset, copySize);
				needCopy -= copySize;
				toOffset += copySize;
				fromOffset = 0;
			} else {
				fromOffset -= sourceSegment.size();
			}
		}
	}

	/**
	 * Increases the capacity to ensure that it can hold at least the
	 * minimum capacity argument.
	 */
	private void grow(int minCapacity) {
		int oldCapacity = segment.size();
		int newCapacity = oldCapacity + (oldCapacity >> 1);
		if (newCapacity - minCapacity < 0) {
			newCapacity = minCapacity;
		}
		segment = MemorySegmentFactory.wrap(Arrays.copyOf(segment.getArray(), newCapacity));
		afterGrow();
	}

	protected static int roundNumberOfBytesToNearestWord(int numBytes) {
		int remainder = numBytes & 0x07;
		if (remainder == 0) {
			return numBytes;
		} else {
			return numBytes + (8 - remainder);
		}
	}

	private static void writeBytesToFixLenPart(
			MemorySegment segment, int fieldOffset, byte[] bytes, int len) {
		long firstByte = len | 0x80; // first bit is 1, other bits is len
		long sevenBytes = 0L; // real data
		if (BinaryRow.LITTLE_ENDIAN) {
			for (int i = 0; i < len; i++) {
				sevenBytes |= ((0x00000000000000FFL & bytes[i]) << (i * 8L));
			}
		} else {
			for (int i = 0; i < len; i++) {
				sevenBytes |= ((0x00000000000000FFL & bytes[i]) << ((6 - i) * 8L));
			}
		}

		final long offsetAndSize = (firstByte << 56) | sevenBytes;

		segment.putLong(fieldOffset, offsetAndSize);
	}
}
