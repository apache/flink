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
import org.apache.flink.table.runtime.util.SegmentsUtil;

import static org.apache.flink.table.dataformat.BinaryFormat.readBinaryFieldFromSegments;
import static org.apache.flink.table.dataformat.BinaryRow.calculateBitSetWidthInBytes;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Its memory storage structure is exactly the same with {@link BinaryRow}.
 * The only different is that, as {@link NestedRow} is used
 * to store row value in the variable-length part of {@link BinaryRow},
 * every field (including both fixed-length part and variable-length part) of {@link NestedRow}
 * has a possibility to cross the boundary of a segment, while the fixed-length part of {@link BinaryRow}
 * must fit into its first memory segment.
 */
public final class NestedRow extends BinarySection implements BaseRow {

	private final int arity;
	private final int nullBitsSizeInBytes;

	public NestedRow(int arity) {
		checkArgument(arity >= 0);
		this.arity = arity;
		this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
	}

	static NestedRow readNestedRowFieldFromSegments(
			MemorySegment[] segments, int numFields, int baseOffset, long offsetAndSize) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		NestedRow row = new NestedRow(numFields);
		row.pointTo(segments, offset + baseOffset, size);
		return row;
	}

	private int getFieldOffset(int pos) {
		return offset + nullBitsSizeInBytes + pos * 8;
	}

	private void assertIndexIsValid(int index) {
		assert index >= 0 : "index (" + index + ") should >= 0";
		assert index < arity : "index (" + index + ") should < " + arity;
	}

	@Override
	public int getArity() {
		return arity;
	}

	@Override
	public byte getHeader() {
		return SegmentsUtil.getByte(segments, offset);
	}

	@Override
	public void setHeader(byte header) {
		SegmentsUtil.setByte(segments, offset, header);
	}

	private void setNotNullAt(int i) {
		assertIndexIsValid(i);
		SegmentsUtil.bitUnSet(segments, offset, i + 8);
	}

	/**
	 * See {@link BinaryRow#setNullAt(int)}.
	 */
	@Override
	public void setNullAt(int i) {
		assertIndexIsValid(i);
		SegmentsUtil.bitSet(segments, offset, i + 8);
		SegmentsUtil.setLong(segments, getFieldOffset(i), 0);
	}

	@Override
	public void setInt(int pos, int value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setInt(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setLong(int pos, long value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setLong(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setDouble(int pos, double value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setDouble(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setDecimal(int pos, Decimal value, int precision) {
		assertIndexIsValid(pos);

		if (Decimal.isCompact(precision)) {
			// compact format
			setLong(pos, value.toUnscaledLong());
		} else {
			int fieldOffset = getFieldOffset(pos);
			int cursor = (int) (SegmentsUtil.getLong(segments, fieldOffset) >>> 32);
			assert cursor > 0 : "invalid cursor " + cursor;
			// zero-out the bytes
			SegmentsUtil.setLong(segments, offset + cursor, 0L);
			SegmentsUtil.setLong(segments, offset + cursor + 8, 0L);

			if (value == null) {
				setNullAt(pos);
				// keep the offset for future update
				SegmentsUtil.setLong(segments, fieldOffset, ((long) cursor) << 32);
			} else {

				byte[] bytes = value.toUnscaledBytes();
				assert (bytes.length <= 16);

				// Write the bytes to the variable length portion.
				SegmentsUtil.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
				setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
			}
		}
	}

	@Override
	public void setTimestamp(int pos, SqlTimestamp value, int precision) {
		assertIndexIsValid(pos);

		if (SqlTimestamp.isCompact(precision)) {
			setLong(pos, value.getMillisecond());
		} else {
			int fieldOffset = getFieldOffset(pos);
			int cursor = (int) (SegmentsUtil.getLong(segments, fieldOffset) >>> 32);
			assert cursor > 0 : "invalid cursor " + cursor;

			if (value == null) {
				setNullAt(pos);
				// zero-out the bytes
				SegmentsUtil.setLong(segments, offset + cursor, 0L);
				SegmentsUtil.setLong(segments, fieldOffset, ((long) cursor) << 32);
			} else {
				// write millisecond to variable length portion.
				SegmentsUtil.setLong(segments, offset + cursor, value.getMillisecond());
				// write nanoOfMillisecond to fixed-length portion.
				setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
			}
		}
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setBoolean(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setShort(int pos, short value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setShort(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setByte(int pos, byte value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setByte(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setFloat(int pos, float value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setFloat(segments, getFieldOffset(pos), value);
	}

	@Override
	public boolean isNullAt(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.bitGet(segments, offset, pos + 8);
	}

	@Override
	public boolean getBoolean(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getBoolean(segments, getFieldOffset(pos));
	}

	@Override
	public byte getByte(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getByte(segments, getFieldOffset(pos));
	}

	@Override
	public short getShort(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getShort(segments, getFieldOffset(pos));
	}

	@Override
	public int getInt(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getInt(segments, getFieldOffset(pos));
	}

	@Override
	public long getLong(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getLong(segments, getFieldOffset(pos));
	}

	@Override
	public float getFloat(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getFloat(segments, getFieldOffset(pos));
	}

	@Override
	public double getDouble(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getDouble(segments, getFieldOffset(pos));
	}

	@Override
	public BinaryString getString(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getFieldOffset(pos);
		final long offsetAndLen = SegmentsUtil.getLong(segments, fieldOffset);
		return BinaryFormat.readBinaryStringFieldFromSegments(segments, offset, fieldOffset, offsetAndLen);
	}

	@Override
	public Decimal getDecimal(int pos, int precision, int scale) {
		assertIndexIsValid(pos);

		if (Decimal.isCompact(precision)) {
			return Decimal.fromUnscaledLong(precision, scale,
					SegmentsUtil.getLong(segments, getFieldOffset(pos)));
		}

		int fieldOffset = getFieldOffset(pos);
		final long offsetAndSize = SegmentsUtil.getLong(segments, fieldOffset);
		return Decimal.readDecimalFieldFromSegments(segments, offset, offsetAndSize, precision, scale);
	}

	@Override
	public SqlTimestamp getTimestamp(int pos, int precision) {
		assertIndexIsValid(pos);

		if (SqlTimestamp.isCompact(precision)) {
			return SqlTimestamp.fromEpochMillis(SegmentsUtil.getLong(segments, getFieldOffset(pos)));
		}

		int fieldOffset = getFieldOffset(pos);
		final long offsetAndNanoOfMilli = SegmentsUtil.getLong(segments, fieldOffset);
		return SqlTimestamp.readTimestampFieldFromSegments(segments, offset, offsetAndNanoOfMilli);
	}

	@Override
	public <T> BinaryGeneric<T> getGeneric(int pos) {
		assertIndexIsValid(pos);
		return BinaryGeneric.readBinaryGenericFieldFromSegments(segments, offset, getLong(pos));
	}

	@Override
	public byte[] getBinary(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getFieldOffset(pos);
		final long offsetAndLen = SegmentsUtil.getLong(segments, fieldOffset);
		return readBinaryFieldFromSegments(segments, offset, fieldOffset, offsetAndLen);
	}

	@Override
	public BaseRow getRow(int pos, int numFields) {
		assertIndexIsValid(pos);
		return NestedRow.readNestedRowFieldFromSegments(segments, numFields, offset, getLong(pos));
	}

	@Override
	public BaseArray getArray(int pos) {
		assertIndexIsValid(pos);
		return BinaryArray.readBinaryArrayFieldFromSegments(segments, offset, getLong(pos));
	}

	@Override
	public BaseMap getMap(int pos) {
		assertIndexIsValid(pos);
		return BinaryMap.readBinaryMapFieldFromSegments(segments, offset, getLong(pos));
	}

	public NestedRow copy() {
		return copy(new NestedRow(arity));
	}

	public NestedRow copy(BaseRow reuse) {
		return copyInternal((NestedRow) reuse);
	}

	private NestedRow copyInternal(NestedRow reuse) {
		byte[] bytes = SegmentsUtil.copyToBytes(segments, offset, sizeInBytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
		return reuse;
	}

	@Override
	public int hashCode() {
		return SegmentsUtil.hashByWords(segments, offset, sizeInBytes);
	}
}
