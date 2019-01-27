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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentWritable;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.runtime.util.StringUtf8Utils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import static org.apache.flink.table.runtime.util.StringUtf8Utils.MAX_BYTES_PER_CHAR;

/**
 * Abstract BinaryWriter.
 */
public abstract class BinaryWriter {

	protected MemorySegment segment;
	protected int cursor;
	protected DataOutputViewStreamWrapper outputView;

	public BinaryWriter() {
		this.outputView = new DataOutputViewStreamWrapper(new BinaryRowWriterOutputView());
	}

	public static int roundNumberOfBytesToNearestWord(int numBytes) {
		int remainder = numBytes & 0x07;
		if (remainder == 0) {
			return numBytes;
		} else {
			return numBytes + (8 - remainder);
		}
	}

	public abstract void setOffsetAndSize(int pos, int offset, long size);

	public abstract int getFieldOffset(int pos);

	/**
	 * See {@link BinaryRow#getBinaryString(int)}.
	 */
	public void writeString(int pos, String input) {
		byte[] bytes = StringUtf8Utils.allocateBytes(input.length() * MAX_BYTES_PER_CHAR);
		int len = StringUtf8Utils.encodeUTF8(input, bytes);
		if (len <= 7) {
			writeLittleBytes(segment, getFieldOffset(pos), bytes, len);
		} else {
			writeBigBytes(pos, bytes, len);
		}
	}

	/**
	 * See {@link BinaryRow#getByteArray(int)}.
	 */
	public void writeByteArray(int pos, byte[] input) {
		int len = input.length;
		if (len <= 7) {
			writeLittleBytes(segment, getFieldOffset(pos), input, len);
		} else {
			writeBigBytes(pos, input, len);
		}
	}

	public void writeBinaryString(int pos, BinaryString input) {
		if (input.isEncoded()) {
			if (input.numBytes() <= 7) {
				byte[] bytes = StringUtf8Utils.allocateBytes(input.numBytes());
				input.copyTo(bytes);
				writeLittleBytes(segment, getFieldOffset(pos), bytes, input.numBytes());
			} else {
				writeBigBinaryString(pos, input);
			}
		} else {
			writeString(pos, input.toString());
		}
	}

	public abstract void writeDecimal(int pos, Decimal value, int precision, int scale);

	public void writeBinaryArray(int pos, BinaryArray value) {
		writeSegments(pos, value.getSegments(), value.getBaseOffset(), value.getSizeInBytes());
	}

	public void writeBinaryMap(int pos, BinaryMap value) {
		writeSegments(pos, value.getSegments(), value.getBaseOffset(), value.getSizeInBytes());
	}

	public void writeBinaryRow(int pos, BinaryRow value) {
		writeSegments(pos, value.getAllSegments(), value.getBaseOffset(), value.getSizeInBytes());
	}

	public void writeNestedRow(int pos, NestedRow value) {
		writeSegments(pos, value.getSegments(), value.getBaseOffset(), value.getSizeInBytes());
	}

	public void writeSegments(int pos, MemorySegment[] segments, int offset, int size) {
		final int roundedSize = roundNumberOfBytesToNearestWord(size);

		// grow the global buffer before writing data.
		ensureCapacity(roundedSize);

		zeroOutPaddingBytes(size);

		if (segments.length == 1) {
			segments[0].copyTo(offset, segment, cursor, size);
		} else {
			writeSegsSlow(segments, offset, size);
		}

		setOffsetAndSize(pos, cursor, size);

		// move the cursor forward.
		cursor += roundedSize;
	}

	@VisibleForTesting
	void writeBigBytes(int pos, byte[] bytes) {
		writeBigBytes(pos, bytes, bytes.length);
	}

	private void writeBigBytes(int pos, byte[] bytes, int len) {
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

	private void writeBigBinaryString(int pos, BinaryString str) {
		int size = str.numBytes();
		final int roundedSize = roundNumberOfBytesToNearestWord(size);

		// grow the global buffer before writing data.
		ensureCapacity(roundedSize);

		zeroOutPaddingBytes(size);

		if (str.getSegments().length == 1) {
			str.getSegments()[0].copyTo(str.getOffset(), segment, cursor, str.numBytes());
		} else {
			writeSegsSlow(str.getSegments(), str.getOffset(), size);
		}

		setOffsetAndSize(pos, cursor, size);

		// move the cursor forward.
		cursor += roundedSize;
	}

	private void writeSegsSlow(MemorySegment[] segments, int offset, int size) {
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

	public static void writeLittleBytes(
			MemorySegment segment, int fieldOffset, byte[] bytes, int len) {
		long firstByte = len | 0x80;
		long sevenBytes = 0L;
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

	public <T> void writeGeneric(int pos, T t, GenericType<T> type) {
		writeGeneric(pos, t, type.getSerializer());
	}

	public <T> void writeGeneric(int pos, T t, TypeSerializer<T> serializer) {
		int beforeCursor = cursor;
		try {
			serializer.serialize(t, outputView);
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
	}

	public abstract void writeBoolean(int pos, boolean value);

	public abstract void writeByte(int pos, byte value);

	public abstract void writeShort(int pos, short value);

	public abstract void writeInt(int pos, int value);

	public abstract void writeLong(int pos, long value);

	public abstract void writeFloat(int pos, float value);

	public abstract void writeDouble(int pos, double value);

	public abstract void writeChar(int pos, char value);

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
		segment = MemorySegmentFactory.wrap(Arrays.copyOf(segment.getHeapMemory(), newCapacity));
		afterGrow();
	}

	public abstract void afterGrow();

	/**
	 * OutputView for write Generic.
	 */
	private class BinaryRowWriterOutputView extends OutputStream implements MemorySegmentWritable {

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

		@Override
		public void write(MemorySegment seg, int off, int len) throws IOException {
			ensureCapacity(len);
			seg.copyTo(off, segment, cursor, len);
			cursor += len;
		}
	}
}
