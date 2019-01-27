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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;
import org.apache.flink.table.dataformat.util.MultiSegUtil;
import org.apache.flink.table.util.hash.Murmur32;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.table.dataformat.BinaryRow.calculateBitSetWidthInBytes;
import static org.apache.flink.table.dataformat.BinaryRow.getBinaryStringFromSeg;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Its memory storage structure and BinaryRow exactly the same, the only different is it supports
 * all bytes in variable MemorySegments.
 */
public final class NestedRow implements BaseRow {

	private final int arity;
	private final int nullBitsSizeInBytes;

	private MemorySegment[] segments;
	private int baseOffset;
	private int sizeInBytes;

	public NestedRow(int arity) {
		checkArgument(arity >= 0);
		this.arity = arity;
		this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity);
	}

	private int getFieldOffset(int pos) {
		return baseOffset + nullBitsSizeInBytes + pos * 8;
	}

	private void assertIndexIsValid(int index) {
		assert index >= 0 : "index (" + index + ") should >= 0";
		assert index < arity : "index (" + index + ") should < " + arity;
	}

	public int getBaseOffset() {
		return baseOffset;
	}

	public int getSizeInBytes() {
		return sizeInBytes;
	}

	public MemorySegment[] getSegments() {
		return segments;
	}

	@Override
	public int getArity() {
		return arity;
	}

	@Override
	public byte getHeader() {
		return MultiSegUtil.getByte(segments, baseOffset);
	}

	@Override
	public void setHeader(byte header) {
		throw new UnsupportedOperationException();
	}

	public void pointTo(MemorySegment segment, int baseOffset, int sizeInBytes) {
		pointTo(new MemorySegment[]{segment}, baseOffset, sizeInBytes);
	}

	public void pointTo(MemorySegment[] segments, int baseOffset, int sizeInBytes) {
		this.segments = segments;
		this.baseOffset = baseOffset;
		this.sizeInBytes = sizeInBytes;
	}

	private void setNotNullAt(int i) {
		assertIndexIsValid(i);
		MultiSegUtil.bitUnSet(segments, baseOffset, i + 8);
	}

	/**
	 * See {@link BinaryRow#setNullAt(int)}.
	 */
	@Override
	public void setNullAt(int i) {
		assertIndexIsValid(i);
		MultiSegUtil.bitSet(segments, baseOffset, i + 8);
		MultiSegUtil.setLong(segments, getFieldOffset(i), 0);
	}

	@Override
	public void setInt(int pos, int value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		MultiSegUtil.setInt(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setLong(int pos, long value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		MultiSegUtil.setLong(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setDouble(int pos, double value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		MultiSegUtil.setDouble(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setChar(int pos, char value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		MultiSegUtil.setChar(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setDecimal(int pos, Decimal value, int precision, int scale) {
		assertIndexIsValid(pos);

		if (Decimal.isCompact(precision)) {
			// compact format
			if (value == null) {
				setNullAt(pos);
			} else {
				setLong(pos, value.toUnscaledLong());
			}
		} else {
			int fieldOffset = getFieldOffset(pos);
			int cursor = (int) (MultiSegUtil.getLong(segments, fieldOffset) >>> 32);
			assert cursor > 0 : "invalid cursor " + cursor;
			// zero-out the bytes
			MultiSegUtil.setLong(segments, baseOffset + cursor, 0L);
			MultiSegUtil.setLong(segments, baseOffset + cursor + 8, 0L);

			if (value == null) {
				setNullAt(pos);
				// keep the offset for future update
				MultiSegUtil.setLong(segments, fieldOffset, ((long) cursor) << 32);
			} else {

				byte[] bytes = value.toUnscaledBytes();
				assert (bytes.length <= 16);

				// Write the bytes to the variable length portion.
				BinaryRowUtil.copyFromBytes(segments, baseOffset + cursor, bytes, 0, bytes.length);
				setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
			}
		}
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		MultiSegUtil.setBoolean(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setShort(int pos, short value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		MultiSegUtil.setShort(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setByte(int pos, byte value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		MultiSegUtil.setByte(segments, getFieldOffset(pos), value);
	}

	@Override
	public void setFloat(int pos, float value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		MultiSegUtil.setFloat(segments, getFieldOffset(pos), value);
	}

	@Override
	public boolean isNullAt(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.bitGet(segments, baseOffset, pos + 8);
	}

	@Override
	public boolean getBoolean(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getBoolean(segments, getFieldOffset(pos));
	}

	@Override
	public byte getByte(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getByte(segments, getFieldOffset(pos));
	}

	@Override
	public short getShort(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getShort(segments, getFieldOffset(pos));
	}

	@Override
	public int getInt(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getInt(segments, getFieldOffset(pos));
	}

	@Override
	public long getLong(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getLong(segments, getFieldOffset(pos));
	}

	@Override
	public float getFloat(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getFloat(segments, getFieldOffset(pos));
	}

	@Override
	public double getDouble(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getDouble(segments, getFieldOffset(pos));
	}

	@Override
	public char getChar(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getChar(segments, getFieldOffset(pos));
	}

	@Override
	public BinaryString getBinaryString(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getFieldOffset(pos);
		final long offsetAndSize = MultiSegUtil.getLong(segments, fieldOffset);
		BinaryString ret = new BinaryString();
		getBinaryStringFromSeg(segments, baseOffset, fieldOffset, offsetAndSize, ret);
		return ret;
	}

	@Override
	public BinaryString getBinaryString(int pos, BinaryString reuse) {
		assertIndexIsValid(pos);
		int fieldOffset = getFieldOffset(pos);
		final long offsetAndSize = MultiSegUtil.getLong(segments, fieldOffset);
		getBinaryStringFromSeg(segments, baseOffset, fieldOffset, offsetAndSize, reuse);
		return reuse;
	}

	@Override
	public Decimal getDecimal(int pos, int precision, int scale) {
		assertIndexIsValid(pos);

		if (Decimal.isCompact(precision)) {
			long longVal = MultiSegUtil.getLong(segments, getFieldOffset(pos));
			return Decimal.fromUnscaledLong(precision, scale, longVal);
		}

		int fieldOffset = getFieldOffset(pos);
		final long offsetAndSize = MultiSegUtil.getLong(segments, fieldOffset);
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		byte[] bytes = BinaryRowUtil.copy(segments, baseOffset + offset, size);
		return Decimal.fromUnscaledBytes(precision, scale, bytes);
	}

	@Override
	public <T> T getGeneric(int pos, TypeSerializer<T> serializer) {
		final long offsetAndSize = getLong(pos);
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		try {
			return InstantiationUtil.deserializeFromByteArray(
					serializer, BinaryRowUtil.copy(segments, baseOffset + offset, size));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T> T getGeneric(int pos, GenericType<T> type) {
		return getGeneric(pos, type.getSerializer());
	}

	@Override
	public BaseRow getBaseRow(int ordinal, int numFields) {
		final long offsetAndSize = getLong(ordinal);
		return BinaryRow.getBaseRow(segments, baseOffset, offsetAndSize, numFields);
	}

	@Override
	public BaseArray getBaseArray(int ordinal) {
		final long offsetAndSize = getLong(ordinal);
		return BinaryRow.getBinaryArray(segments, baseOffset, offsetAndSize);
	}

	@Override
	public BaseMap getBaseMap(int ordinal) {
		final long offsetAndSize = getLong(ordinal);
		return BinaryRow.getBinaryMap(segments, baseOffset, offsetAndSize);
	}

	@Override
	public byte[] getByteArray(int ordinal) {
		int fieldOffset = getFieldOffset(ordinal);
		final long offsetAndSize = MultiSegUtil.getLong(segments, fieldOffset);
		return BinaryRow.getByteArray(segments, baseOffset, fieldOffset, offsetAndSize);
	}

	public NestedRow copy() {
		return copy(new NestedRow(arity));
	}

	public NestedRow copy(BaseRow reuse) {
		return copyInternal((NestedRow) reuse);
	}

	private NestedRow copyInternal(NestedRow reuse) {
		byte[] bytes = BinaryRowUtil.copy(segments, baseOffset, sizeInBytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
		return reuse;
	}

	@Override
	public boolean equals(Object o) {
		return equalsFrom(o, 0);
	}

	@Override
	public boolean equalsWithoutHeader(BaseRow o) {
		throw new UnsupportedOperationException();
	}

	private boolean equalsFrom(Object o, int startIndex) {
		if (o != null && o instanceof NestedRow) {
			NestedRow other = (NestedRow) o;
			return sizeInBytes == other.sizeInBytes && BinaryRowUtil.equals(
				segments,
				baseOffset + startIndex,
				other.segments,
				other.baseOffset + startIndex,
				sizeInBytes - startIndex);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		if (segments.length == 1) {
			return Murmur32.hashBytesByWords(segments[0], baseOffset, sizeInBytes, 42);
		} else {
			return hashSlow();
		}
	}

	private int hashSlow() {
		byte[] bytes = new byte[sizeInBytes];
		BinaryRowUtil.copySlow(segments, baseOffset, bytes, sizeInBytes);
		return Murmur32.hashBytesByWords(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes, 42);
	}
}
