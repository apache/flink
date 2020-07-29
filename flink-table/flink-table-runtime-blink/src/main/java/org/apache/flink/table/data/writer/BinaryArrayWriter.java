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

package org.apache.flink.table.data.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.binary.BinaryArrayData;
import org.apache.flink.table.data.binary.BinarySegmentUtils;

/**
 * Writer for binary array. See {@link BinaryArrayData}.
 */
@Internal
public final class BinaryArrayWriter extends AbstractBinaryWriter {

	private final int nullBitsSizeInBytes;
	private final BinaryArrayData array;
	private final int numElements;
	private final int fixedSize;

	public BinaryArrayWriter(BinaryArrayData array, int numElements, int elementSize) {
		this.nullBitsSizeInBytes = BinaryArrayData.calculateHeaderInBytes(numElements);
		this.fixedSize = roundNumberOfBytesToNearestWord(
			nullBitsSizeInBytes + elementSize * numElements);
		this.cursor = fixedSize;
		this.numElements = numElements;

		this.segment = MemorySegmentFactory.wrap(new byte[fixedSize]);
		this.segment.putInt(0, numElements);
		this.array = array;
	}

	public int getNumElements() {
		return numElements;
	}

	private int getElementOffset(int pos, int elementSize) {
		return nullBitsSizeInBytes + elementSize * pos;
	}

	@Override
	protected int getFieldOffset(int pos) {
		return getElementOffset(pos, 8);
	}

	@Override
	protected void setOffsetAndSize(int pos, int offset, long size) {
		final long offsetAndSize = ((long) offset << 32) | size;
		segment.putLong(getElementOffset(pos, 8), offsetAndSize);
	}

	@Override
	protected void afterGrow() {
		array.pointTo(segment, 0, segment.size());
	}

	// --------------------------------------------------------------------------------------------

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

	// --------------------------------------------------------------------------------------------

	@Override
	protected void setNullBit(int pos) {
		BinarySegmentUtils.bitSet(segment, 4, pos);
	}

	@Override
	public void writeNullBoolean(int pos) {
		setNullBit(pos);
		// put zero into the corresponding field when set null
		segment.putBoolean(getElementOffset(pos, 1), false);
	}

	@Override
	public void writeNullByte(int pos) {
		setNullBit(pos);
		// put zero into the corresponding field when set null
		segment.put(getElementOffset(pos, 1), (byte) 0);
	}

	@Override
	public void writeNullShort(int pos) {
		setNullBit(pos);
		// put zero into the corresponding field when set null
		segment.putShort(getElementOffset(pos, 2), (short) 0);
	}

	@Override
	public void writeNullInt(int pos) {
		setNullBit(pos);
		// put zero into the corresponding field when set null
		segment.putInt(getElementOffset(pos, 4), 0);
	}

	@Override
	public void writeNullLong(int pos) {
		setNullBit(pos);
		// put zero into the corresponding field when set null
		segment.putLong(getElementOffset(pos, 8), 0);
	}

	@Override
	public void writeNullFloat(int pos) {
		setNullBit(pos);
		// put zero into the corresponding field when set null
		segment.putFloat(getElementOffset(pos, 4), (float) 0);
	}

	@Override
	public void writeNullDouble(int pos) {
		setNullBit(pos);
		// put zero into the corresponding field when set null
		segment.putDouble(getElementOffset(pos, 8), 0);
	}

	@Override
	public void writeNullString(int pos) {
		writeNullLong(pos);
	}

	@Override
	public void writeNullBinary(int pos) {
		writeNullLong(pos);
	}

	@Override
	public void writeNullArray(int pos) {
		writeNullLong(pos);
	}

	@Override
	public void writeNullMap(int pos) {
		writeNullLong(pos);
	}

	@Override
	public void writeNullRow(int pos) {
		writeNullLong(pos);
	}

	@Override
	public void writeNullRawValue(int pos) {
		writeNullLong(pos);
	}

	// --------------------------------------------------------------------------------------------

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

	// --------------------------------------------------------------------------------------------

	/**
	 * Finally, complete write to set real size to row.
	 */
	@Override
	public void complete() {
		array.pointTo(segment, 0, cursor);
	}
}
