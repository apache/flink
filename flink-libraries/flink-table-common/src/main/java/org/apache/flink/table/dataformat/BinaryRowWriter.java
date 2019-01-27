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
import org.apache.flink.table.dataformat.util.BitSetUtil;

/**
 * Use the special format to write data to a {@link MemorySegment} (its capacity grows
 * automatically).
 *
 * <p>If write a Row:
 * 1. New a writer.
 * 2. Write each field by writeXX or setNullAt. (Variable length fields can not be written
 * repeatedly.)
 * 3. Invoke {@link #complete()}.
 *
 * <p>If want to reuse this writer, please invoke {@link #reset()} first.
 */
public class BinaryRowWriter extends BinaryWriter {

	private final int nullBitsSizeInBytes;
	private final BinaryRow row;
	private final int fixedSize;

	public BinaryRowWriter(BinaryRow row) {
		this(row, 0);
	}

	public BinaryRowWriter(BinaryRow row, int initialSize) {
		this.nullBitsSizeInBytes = BinaryRow.calculateBitSetWidthInBytes(row.getArity());
		this.fixedSize = row.getFixedLengthPartSize();
		this.cursor = fixedSize;

		this.segment = MemorySegmentFactory.wrap(new byte[fixedSize + initialSize]);
		this.row = row;
		this.row.pointTo(segment, 0, segment.size());
	}

	/**
	 * First, reset.
	 */
	public void reset() {
		resetCursor();
		for (int i = 0; i < nullBitsSizeInBytes; i += 8) {
			segment.putLong(i, 0L);
		}
	}

	public void resetCursor() {
		this.cursor = fixedSize;
	}

	/**
	 * Default not null.
	 */
	public void setNullAt(int pos) {
		// need add header 8 bit.
		BitSetUtil.set(segment, 0, pos + 8);
		segment.putLong(getFieldOffset(pos), 0L);
	}

	@Override
	public int getFieldOffset(int pos) {
		return nullBitsSizeInBytes + 8 * pos;
	}

	@Override
	public void setOffsetAndSize(int pos, int offset, long size) {
		final long offsetAndSize = ((long) offset << 32) | size;
		segment.putLong(getFieldOffset(pos), offsetAndSize);
	}

	public void writeHeader(byte header) {
		segment.put(0, header);
	}

	@Override
	public void writeBoolean(int pos, boolean value) {
		segment.putBoolean(getFieldOffset(pos), value);
	}

	@Override
	public void writeByte(int pos, byte value) {
		segment.put(getFieldOffset(pos), value);
	}

	@Override
	public void writeShort(int pos, short value) {
		segment.putShort(getFieldOffset(pos), value);
	}

	@Override
	public void writeInt(int pos, int value) {
		segment.putInt(getFieldOffset(pos), value);
	}

	@Override
	public void writeLong(int pos, long value) {
		segment.putLong(getFieldOffset(pos), value);
	}

	@Override
	public void writeFloat(int pos, float value) {
		if (Float.isNaN(value)) {
			value = Float.NaN;
		}
		segment.putFloat(getFieldOffset(pos), value);
	}

	@Override
	public void writeDouble(int pos, double value) {
		if (Double.isNaN(value)) {
			value = Double.NaN;
		}
		segment.putDouble(getFieldOffset(pos), value);
	}

	@Override
	public void writeChar(int pos, char value) {
		segment.putChar(getFieldOffset(pos), value);
	}

	@Override
	public void writeDecimal(int pos, Decimal value, int precision, int scale) {
		assert value == null || (value.getPrecision() == precision && value.getScale() == scale);

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
				// need add header 8 bit.
				BitSetUtil.set(segment, 0, pos + 8);
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

	@Override
	public void afterGrow() {
		row.pointTo(segment, 0, segment.size());
	}

	/**
	 * Finally, complete write to set real size to row.
	 */
	public void complete() {
		row.setTotalSize(cursor);
	}
}
