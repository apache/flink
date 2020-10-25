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

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import static org.apache.flink.table.data.binary.BinaryRowData.calculateBitSetWidthInBytes;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Its memory storage structure is exactly the same with {@link BinaryRowData}.
 * The only different is that, as {@link NestedRowData} is used
 * to store row value in the variable-length part of {@link BinaryRowData},
 * every field (including both fixed-length part and variable-length part) of {@link NestedRowData}
 * has a possibility to cross the boundary of a segment, while the fixed-length part of {@link BinaryRowData}
 * must fit into its first memory segment.
 */
@Internal
public final class NestedRowData extends BinarySection implements RowData, TypedSetters {

	private final int arity;
	private final int nullBitsSizeInBytes;

	public NestedRowData(int arity) {
		checkArgument(arity >= 0);
		this.arity = arity;
		this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
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
	public RowKind getRowKind() {
		byte kindValue = BinarySegmentUtils.getByte(segments, offset);
		return RowKind.fromByteValue(kindValue);
	}

	@Override
	public void setRowKind(RowKind kind) {
		BinarySegmentUtils.setByte(segments, offset, kind.toByteValue());
	}

	private void setNotNullAt(int i) {
		assertIndexIsValid(i);
		BinarySegmentUtils.bitUnSet(segments, offset, i + 8);
	}

	/**
	 * See {@link BinaryRowData#setNullAt(int)}.
	 */
	@Override
	public void setNullAt(int i) {
		assertIndexIsValid(i);
		BinarySegmentUtils.bitSet(segments, offset, i + 8);
		BinarySegmentUtils.setLong(segments, getFieldOffset(i), 0);
	}

	@Override
	public void setInt(int pos, int value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		BinarySegmentUtils.setInt(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setLong(int pos, long value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		BinarySegmentUtils.setLong(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setDouble(int pos, double value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		BinarySegmentUtils.setDouble(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setDecimal(int pos, DecimalData value, int precision) {
		assertIndexIsValid(pos);

		if (DecimalData.isCompact(precision)) {
			// compact format
			setLong(pos, value.toUnscaledLong());
		} else {
			int fieldOffset = getFieldOffset(pos);
			int cursor = (int) (BinarySegmentUtils.getLong(segments, fieldOffset) >>> 32);
			assert cursor > 0 : "invalid cursor " + cursor;
			// zero-out the bytes
			BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
			BinarySegmentUtils.setLong(segments, offset + cursor + 8, 0L);

			if (value == null) {
				setNullAt(pos);
				// keep the offset for future update
				BinarySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
			} else {

				byte[] bytes = value.toUnscaledBytes();
				assert (bytes.length <= 16);

				// Write the bytes to the variable length portion.
				BinarySegmentUtils.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
				setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
			}
		}
	}

	@Override
	public void setTimestamp(int pos, TimestampData value, int precision) {
		assertIndexIsValid(pos);

		if (TimestampData.isCompact(precision)) {
			setLong(pos, value.getMillisecond());
		} else {
			int fieldOffset = getFieldOffset(pos);
			int cursor = (int) (BinarySegmentUtils.getLong(segments, fieldOffset) >>> 32);
			assert cursor > 0 : "invalid cursor " + cursor;

			if (value == null) {
				setNullAt(pos);
				// zero-out the bytes
				BinarySegmentUtils.setLong(segments, offset + cursor, 0L);
				BinarySegmentUtils.setLong(segments, fieldOffset, ((long) cursor) << 32);
			} else {
				// write millisecond to variable length portion.
				BinarySegmentUtils.setLong(segments, offset + cursor, value.getMillisecond());
				// write nanoOfMillisecond to fixed-length portion.
				setLong(pos, ((long) cursor << 32) | (long) value.getNanoOfMillisecond());
			}
		}
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		BinarySegmentUtils.setBoolean(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setShort(int pos, short value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		BinarySegmentUtils.setShort(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setByte(int pos, byte value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		BinarySegmentUtils.setByte(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setFloat(int pos, float value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		BinarySegmentUtils.setFloat(segments, getFieldOffset(pos), value);
	}

	@Override
	public boolean isNullAt(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.bitGet(segments, offset, pos + 8);
	}

	@Override
	public boolean getBoolean(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.getBoolean(segments, getFieldOffset(pos));
	}

	@Override
	public byte getByte(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.getByte(segments, getFieldOffset(pos));
	}

	@Override
	public short getShort(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.getShort(segments, getFieldOffset(pos));
	}

	@Override
	public int getInt(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.getInt(segments, getFieldOffset(pos));
	}

	@Override
	public long getLong(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.getLong(segments, getFieldOffset(pos));
	}

	@Override
	public float getFloat(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.getFloat(segments, getFieldOffset(pos));
	}

	@Override
	public double getDouble(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.getDouble(segments, getFieldOffset(pos));
	}

	@Override
	public StringData getString(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getFieldOffset(pos);
		final long offsetAndLen = BinarySegmentUtils.getLong(segments, fieldOffset);
		return BinarySegmentUtils.readStringData(segments, offset, fieldOffset, offsetAndLen);
	}

	@Override
	public DecimalData getDecimal(int pos, int precision, int scale) {
		assertIndexIsValid(pos);

		if (DecimalData.isCompact(precision)) {
			return DecimalData.fromUnscaledLong(
				BinarySegmentUtils.getLong(segments, getFieldOffset(pos)),
				precision,
				scale);
		}

		int fieldOffset = getFieldOffset(pos);
		final long offsetAndSize = BinarySegmentUtils.getLong(segments, fieldOffset);
		return BinarySegmentUtils.readDecimalData(segments, offset, offsetAndSize, precision, scale);
	}

	@Override
	public TimestampData getTimestamp(int pos, int precision) {
		assertIndexIsValid(pos);

		if (TimestampData.isCompact(precision)) {
			return TimestampData.fromEpochMillis(BinarySegmentUtils.getLong(segments, getFieldOffset(pos)));
		}

		int fieldOffset = getFieldOffset(pos);
		final long offsetAndNanoOfMilli = BinarySegmentUtils.getLong(segments, fieldOffset);
		return BinarySegmentUtils.readTimestampData(segments, offset, offsetAndNanoOfMilli);
	}

	@Override
	public <T> RawValueData<T> getRawValue(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.readRawValueData(segments, offset, getLong(pos));
	}

	@Override
	public byte[] getBinary(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getFieldOffset(pos);
		final long offsetAndLen = BinarySegmentUtils.getLong(segments, fieldOffset);
		return BinarySegmentUtils.readBinary(segments, offset, fieldOffset, offsetAndLen);
	}

	@Override
	public RowData getRow(int pos, int numFields) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.readRowData(segments, numFields, offset, getLong(pos));
	}

	@Override
	public ArrayData getArray(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.readArrayData(segments, offset, getLong(pos));
	}

	@Override
	public MapData getMap(int pos) {
		assertIndexIsValid(pos);
		return BinarySegmentUtils.readMapData(segments, offset, getLong(pos));
	}

	public NestedRowData copy() {
		return copy(new NestedRowData(arity));
	}

	public NestedRowData copy(RowData reuse) {
		return copyInternal((NestedRowData) reuse);
	}

	private NestedRowData copyInternal(NestedRowData reuse) {
		byte[] bytes = BinarySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
		return reuse;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		// both BinaryRowData and NestedRowData have the same memory format
		if (!(o instanceof NestedRowData || o instanceof BinaryRowData)) {
			return false;
		}
		final BinarySection that = (BinarySection) o;
		return sizeInBytes == that.sizeInBytes &&
			BinarySegmentUtils.equals(segments, offset, that.segments, that.offset, sizeInBytes);
	}

	@Override
	public int hashCode() {
		return BinarySegmentUtils.hashByWords(segments, offset, sizeInBytes);
	}
}
