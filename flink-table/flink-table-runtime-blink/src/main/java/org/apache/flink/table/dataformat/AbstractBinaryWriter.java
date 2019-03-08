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

import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.util.SegmentsUtil;

import java.io.IOException;
import java.io.OutputStream;
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

	protected DataOutputViewStreamWrapper outputView;

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

	protected abstract void setNullBit(int ordinal);

	/**
	 * See {@link BinaryString#readBinaryStringFieldFromSegments}.
	 */
	@Override
	public void writeString(int pos, BinaryString input) {
		if (input.getSegments() == null) {
			String javaObject = input.getJavaObject();
			writeBytes(pos, javaObject.getBytes());
		} else {
			int len = input.getSizeInBytes();
			if (len <= 7) {
				byte[] bytes = SegmentsUtil.allocateReuseBytes(len);
				SegmentsUtil.copyToBytes(input.getSegments(), input.getOffset(), bytes, 0, len);
				writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
			} else {
				writeSegmentsToVarLenPart(pos, input.getSegments(), input.getOffset(), len);
			}
		}
	}

	private void writeBytes(int pos, byte[] bytes) {
		int len = bytes.length;
		if (len <= 7) {
			writeBytesToFixLenPart(segment, getFieldOffset(pos), bytes, len);
		} else {
			writeBytesToVarLenPart(pos, bytes, len);
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

	private DataOutputViewStreamWrapper getOutputView() {
		if (outputView == null) {
			outputView = new DataOutputViewStreamWrapper(new BinaryRowWriterOutputView());
		}
		return outputView;
	}

	@Override
	public void writeGeneric(int pos, BinaryGeneric input) {
		if (input.getSegments() == null) {
			int beforeCursor = cursor;
			try {
				input.getJavaObjectSerializer().serialize(input.getJavaObject(), getOutputView());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			int size = cursor - beforeCursor;
			final int roundedSize = roundNumberOfBytesToNearestWord(size);
			int paddingBytes = roundedSize - size;
			ensureCapacity(paddingBytes);
			setOffsetAndSize(pos, beforeCursor, size);
			zeroBytes(cursor, paddingBytes);
			cursor += paddingBytes;
		} else {
			writeSegmentsToVarLenPart(pos, input.getSegments(), input.getOffset(), input.getSizeInBytes());
		}
	}

	@Override
	public void writeRow(int pos, BaseRow input, BaseRowSerializer serializer) {
		if (input instanceof BinaryFormat) {
			BinaryFormat row = (BinaryFormat) input;
			writeSegmentsToVarLenPart(pos, row.getSegments(), row.getOffset(), row.getSizeInBytes());
		} else {
			writeRow(pos, serializer.baseRowToBinary(input), serializer);
		}
	}

	@Override
	public void writeDecimal(int pos, Decimal value, int precision) {
		assert value == null || (value.getPrecision() == precision);

		if (Decimal.isCompact(precision)) {
			assert value != null;
			writeLong(pos, value.toUnscaledLong());
		} else {
			// grow the global buffer before writing data.
			ensureCapacity(16);

			// zero-out the bytes
			segment.putLong(cursor, 0L);
			segment.putLong(cursor + 8, 0L);

			// Make sure Decimal object has the same scale as DecimalType.
			// Note that we may pass in null Decimal object to set null for it.
			if (value == null) {
				setNullBit(pos);
				// keep the offset for future update
				setOffsetAndSize(pos, cursor, 0);
			} else {
				final byte[] bytes = value.toUnscaledBytes();
				assert bytes.length <= 16;

				// Write the bytes to the variable length portion.
				segment.put(cursor, bytes, 0, bytes.length);
				setOffsetAndSize(pos, cursor, bytes.length);
			}

			// move the cursor forward.
			cursor += 16;
		}
	}

	private void zeroBytes(int offset, int size) {
		for (int i = offset; i < offset + size; i++) {
			segment.put(i, (byte) 0);
		}
	}

	protected void zeroOutPaddingBytes(int numBytes) {
		if ((numBytes & 0x07) > 0) {
			segment.putLong(cursor + ((numBytes >> 3) << 3), 0L);
		}
	}

	protected void ensureCapacity(int neededSize) {
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

	private void writeBytesToVarLenPart(int pos, byte[] bytes, int len) {
		final int roundedSize = roundNumberOfBytesToNearestWord(len);

		// grow the global buffer before writing data.
		ensureCapacity(roundedSize);

		zeroOutPaddingBytes(len);

		// Write the bytes to the variable length portion.
		segment.put(cursor, bytes, 0, len);

		setOffsetAndSize(pos, cursor, len);

		// move the cursor forward.
		cursor += roundedSize;
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

	/**
	 * OutputView for write Generic.
	 */
	private class BinaryRowWriterOutputView extends OutputStream {

		/**
		 * Writes the specified byte to this output stream. The general contract for
		 * <code>write</code> is that one byte is written to the output stream. The byte to be
		 * written is the eight low-order bits of the argument <code>b</code>. The 24 high-order
		 * bits of <code>b</code> are ignored.
		 */
		@Override
		public void write(int b) throws IOException {
			ensureCapacity(1);
			segment.put(cursor, (byte) b);
			cursor += 1;
		}

		@Override
		public void write(byte[] b) throws IOException {
			ensureCapacity(b.length);
			segment.put(cursor, b, 0, b.length);
			cursor += b.length;
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			ensureCapacity(len);
			segment.put(cursor, b, off, len);
			cursor += len;
		}

		public void write(MemorySegment seg, int off, int len) throws IOException {
			ensureCapacity(len);
			seg.copyTo(off, segment, cursor, len);
			cursor += len;
		}
	}
}
