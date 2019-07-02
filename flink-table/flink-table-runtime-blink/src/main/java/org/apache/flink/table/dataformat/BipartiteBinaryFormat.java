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
import org.apache.flink.table.util.SegmentsUtil;

import java.nio.ByteOrder;

/**
 * Binary format that is based on a two part memory layout:
 * 1. The fixed length part contains the memory head, the null bits, and the fixed size data.
 * 2. The variable length part contains data without a fixed length, e.g. strings, byte arrays, etc.
 */
public class BipartiteBinaryFormat extends BinaryFormat implements TypeGetterSetters {
	public static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

	/**
	 * The number of elements.
	 */
	protected int numElements;

	/**
	 * The size of the header in bytes.
	 */
	protected final int headerSizeInBytes;

	/**
	 * The size (in bytes) of the null bits plus the header.
	 */
	protected int nullBitsSizeInBytes;

	/**
	 * The unit for the null bits and header.
	 * That is, the total size (in bytes) of the null bits plus the header must be rounded up to this value.
	 */
	protected final int nullBitsUnitInBytes;

	/**
	 * The size of fixed length element.
	 */
	protected int fixedElementSizeInBytes;

	public BipartiteBinaryFormat(int headerSizeInBytes, int nullBitsUnitInBytes) {
		this.headerSizeInBytes = headerSizeInBytes;
		this.nullBitsUnitInBytes = nullBitsUnitInBytes;
	}

	public BipartiteBinaryFormat(
		MemorySegment[] segments,
		int offset, int sizeInBytes,
		int headerSizeInBytes,
		int nullBitsUnitInBytes) {
		super(segments, offset, sizeInBytes);
		this.headerSizeInBytes = headerSizeInBytes;
		this.nullBitsUnitInBytes = nullBitsUnitInBytes;
	}

	/**
	 * Calculate the number bytes to store the nullable bits plus the header, rounded up to 8 bytes.
	 * @return the required size.
	 */
	public static int calculateBitSetWidthInBytes(int numElements, int nullBitsUnitInBytes, int headerSizeInBytes) {
		int nullBitsUnitInBits = nullBitsUnitInBytes * 8;
		int headerSizeInBits = headerSizeInBytes * 8;
		return ((numElements + headerSizeInBits + nullBitsUnitInBits - 1) / nullBitsUnitInBits) * nullBitsUnitInBytes;
	}

	public static int calculateFixPartSizeInBytes(
		int numElements, int nullBitsUnitInBytes, int headerSizeInBytes, int fixedElementSizeInBytes) {
		return calculateBitSetWidthInBytes(
			numElements, nullBitsUnitInBytes, headerSizeInBytes) + fixedElementSizeInBytes * numElements;
	}

	protected int getFieldOffset(int pos) {
		return offset + nullBitsSizeInBytes + pos * fixedElementSizeInBytes;
	}

	private void assertIndexIsValid(int index) {
		assert index >= 0 : "index (" + index + ") should >= 0";
		assert index < numElements : "index (" + index + ") should < " + numElements;
	}

	public int getFixedLengthPartSize() {
		return nullBitsSizeInBytes + fixedElementSizeInBytes * numElements;
	}

	@Override
	public boolean isNullAt(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.bitGet(segments, offset + headerSizeInBytes, pos);
	}

	protected void setNotNullAt(int i) {
		assertIndexIsValid(i);
		SegmentsUtil.bitUnSet(segments, offset + headerSizeInBytes, i);
	}

	@Override
	public void setNullAt(int i) {
		assertIndexIsValid(i);
		SegmentsUtil.bitSet(segments, offset  + headerSizeInBytes, i);
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
				assert bytes.length <= 16;

				// Write the bytes to the variable length portion.
				SegmentsUtil.copyFromBytes(segments, offset + cursor, bytes, 0, bytes.length);
				setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
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
		return BinaryString.readBinaryStringFieldFromSegments(segments, offset, fieldOffset, offsetAndLen);
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
	public BinaryArray getArray(int pos) {
		assertIndexIsValid(pos);
		return BinaryArray.readBinaryArrayFieldFromSegments(segments, offset, getLong(pos));
	}

	@Override
	public BinaryMap getMap(int pos) {
		assertIndexIsValid(pos);
		return BinaryMap.readBinaryMapFieldFromSegments(segments, offset, getLong(pos));
	}

	@Override
	public BaseRow getRow(int pos, int numFields) {
		assertIndexIsValid(pos);
		return NestedRow.readNestedRowFieldFromSegments(segments, numFields, offset, getLong(pos));
	}

	/**
	 * The bit is 1 when the field is null. Default is 0.
	 */
	public boolean anyNull() {
		int index = headerSizeInBytes;
		while (index + 8 <= nullBitsSizeInBytes) {
			if (SegmentsUtil.getLong(segments, index) != 0) {
				return true;
			}
			index += 8;
		}

		while (index < nullBitsSizeInBytes) {
			if (SegmentsUtil.getByte(segments, index) != 0) {
				return true;
			}
			index += 1;
		}

		return false;
	}

	public boolean anyNull(int[] fields) {
		for (int field : fields) {
			if (isNullAt(field)) {
				return true;
			}
		}
		return false;
	}

	public void clear() {
		segments = null;
		offset = 0;
		sizeInBytes = 0;
	}

	@Override
	public int hashCode() {
		return SegmentsUtil.hashByWords(segments, offset, sizeInBytes);
	}

	@Override
	public boolean equals(Object o) {
		return equalsFrom(o, 0);
	}

	protected boolean equalsFrom(Object o, int startIndex) {
		if (o == null) {
			return false;
		}

		if (!o.getClass().equals(this.getClass())) {
			return false;
		}

		BipartiteBinaryFormat other = (BipartiteBinaryFormat) o;
		return sizeInBytes == other.sizeInBytes &&
			SegmentsUtil.equals(
				segments, offset + startIndex,
				other.segments, other.offset + startIndex, sizeInBytes - startIndex);
	}

	public int numElements() {
		return numElements;
	}
}
