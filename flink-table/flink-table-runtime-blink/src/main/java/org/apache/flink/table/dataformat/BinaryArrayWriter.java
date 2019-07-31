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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.runtime.util.SegmentsUtil;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Writer for binary array. See {@link BinaryArray}.
 */
public final class BinaryArrayWriter extends AbstractBinaryWriter {

	private final int nullBitsSizeInBytes;
	private final BinaryArray array;
	private final int numElements;
	private int fixedSize;

	public BinaryArrayWriter(BinaryArray array, int numElements, int elementSize) {
		this.nullBitsSizeInBytes = BinaryArray.calculateHeaderInBytes(numElements);
		this.fixedSize = roundNumberOfBytesToNearestWord(
				nullBitsSizeInBytes + elementSize * numElements);
		this.cursor = fixedSize;
		this.numElements = numElements;

		this.segment = MemorySegmentFactory.wrap(new byte[fixedSize]);
		this.segment.putInt(0, numElements);
		this.array = array;
	}

	/**
	 * First, reset.
	 */
	@Override
	public void reset() {
		this.cursor = fixedSize;
		for (int i = 0; i < nullBitsSizeInBytes; i += 8) {
			segment.putLong(i, 0L);
		}
		this.segment.putInt(0, numElements);
	}

	@Override
	public void setNullBit(int ordinal) {
		SegmentsUtil.bitSet(segment, 4, ordinal);
	}

	public void setNullBoolean(int ordinal) {
		setNullBit(ordinal);
		// put zero into the corresponding field when set null
		segment.putBoolean(getElementOffset(ordinal, 1), false);
	}

	public void setNullByte(int ordinal) {
		setNullBit(ordinal);
		// put zero into the corresponding field when set null
		segment.put(getElementOffset(ordinal, 1), (byte) 0);
	}

	public void setNullShort(int ordinal) {
		setNullBit(ordinal);
		// put zero into the corresponding field when set null
		segment.putShort(getElementOffset(ordinal, 2), (short) 0);
	}

	public void setNullInt(int ordinal) {
		setNullBit(ordinal);
		// put zero into the corresponding field when set null
		segment.putInt(getElementOffset(ordinal, 4), 0);
	}

	public void setNullLong(int ordinal) {
		setNullBit(ordinal);
		// put zero into the corresponding field when set null
		segment.putLong(getElementOffset(ordinal, 8), (long) 0);
	}

	public void setNullFloat(int ordinal) {
		setNullBit(ordinal);
		// put zero into the corresponding field when set null
		segment.putFloat(getElementOffset(ordinal, 4), (float) 0);
	}

	public void setNullDouble(int ordinal) {
		setNullBit(ordinal);
		// put zero into the corresponding field when set null
		segment.putDouble(getElementOffset(ordinal, 8), (double) 0);
	}

	@Override
	public void setNullAt(int ordinal) {
		setNullLong(ordinal);
	}

	public void setNullAt(int pos, LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				setNullBoolean(pos);
				break;
			case TINYINT:
				setNullByte(pos);
				break;
			case SMALLINT:
				setNullShort(pos);
				break;
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				setNullInt(pos);
				break;
			case BIGINT:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
			case INTERVAL_DAY_TIME:
				setNullLong(pos);
				break;
			case FLOAT:
				setNullFloat(pos);
				break;
			case DOUBLE:
				setNullDouble(pos);
				break;
			default:
				setNullAt(pos);
		}
	}

	private int getElementOffset(int pos, int elementSize) {
		return nullBitsSizeInBytes + elementSize * pos;
	}

	@Override
	public int getFieldOffset(int pos) {
		return getElementOffset(pos, 8);
	}

	@Override
	public void setOffsetAndSize(int pos, int offset, long size) {
		final long offsetAndSize = ((long) offset << 32) | size;
		segment.putLong(getElementOffset(pos, 8), offsetAndSize);
	}

	@Override
	public void writeBoolean(int pos, boolean value) {
		segment.putBoolean(getElementOffset(pos, 1), value);
	}

	@Override
	public void writeByte(int pos, byte value) {
		segment.put(getElementOffset(pos, 1), value);
	}

	@Override
	public void writeShort(int pos, short value) {
		segment.putShort(getElementOffset(pos, 2), value);
	}

	@Override
	public void writeInt(int pos, int value) {
		segment.putInt(getElementOffset(pos, 4), value);
	}

	@Override
	public void writeLong(int pos, long value) {
		segment.putLong(getElementOffset(pos, 8), value);
	}

	@Override
	public void writeFloat(int pos, float value) {
		if (Float.isNaN(value)) {
			value = Float.NaN;
		}
		segment.putFloat(getElementOffset(pos, 4), value);
	}

	@Override
	public void writeDouble(int pos, double value) {
		if (Double.isNaN(value)) {
			value = Double.NaN;
		}
		segment.putDouble(getElementOffset(pos, 8), value);
	}

	@Override
	public void afterGrow() {
		array.pointTo(segment, 0, segment.size());
	}

	/**
	 * Finally, complete write to set real size to row.
	 */
	@Override
	public void complete() {
		array.pointTo(segment, 0, cursor);
	}

	public int getNumElements() {
		return numElements;
	}
}
