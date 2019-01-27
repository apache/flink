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
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.GenericType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.Types;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;
import org.apache.flink.table.dataformat.util.BitSetUtil;
import org.apache.flink.table.dataformat.util.MultiSegUtil;
import org.apache.flink.table.util.hash.Murmur32;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.util.hash.Murmur32.DEFAULT_SEED;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A special row which is backed by {@link MemorySegment} instead of Object. It can significantly reduce the
 * serialization/deserialization of Java objects.
 *
 * <p>A Row has two part: Fixed-length part and variable-length part.
 *
 * <p>Fixed-length part contains null bit set and field values. Null bit set is used for null tracking and is
 * aligned to 8-byte word boundaries. `Field values` holds fixed-length primitive types and variable-length
 * values which can be stored in 8 bytes inside. If it do not fit the variable-length field, then store the
 * length and offset of variable-length part. Fixed-length part will certainly fall into a MemorySegment,
 * which will speed up the read and write of field.
 *
 * <p>Variable-length part may fall into multiple MemorySegments.
 *
 * <p>{@code BinaryRow} are influenced by Apache Spark UnsafeRow in project tungsten.
 * The difference is that BinaryRow is placed on a discontinuous memory, and the variable length type can
 * also be placed on a fixed length area (If it's short enough).
 */
public final class BinaryRow implements BaseRow {

	public static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

	private static final Set<InternalType> MUTABLE_FIELD_TYPES;

	static {
		MUTABLE_FIELD_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
				Types.BOOLEAN,
				Types.BYTE,
				Types.SHORT,
				Types.INT,
				Types.LONG,
				Types.FLOAT,
				Types.DOUBLE,
				Types.CHAR,
				Types.TIMESTAMP,
				Types.DATE,
				Types.TIME,
				Types.INTERVAL_MONTHS,
				Types.INTERVAL_MILLIS,
				Types.ROWTIME_INDICATOR,
				Types.PROCTIME_INDICATOR)));
	}

	public static int calculateBitSetWidthInBytes(int arity) {
		// add 8 bit header
		return ((arity + 63 + 8) / 64) * 8;
	}

	public static int calculateFixPartSizeInBytes(int arity) {
		return calculateBitSetWidthInBytes(arity) + 8 * arity;
	}

	/**
	 * If it is a fixed-length field, we can call this BinaryRow's setXX method for in-place updates.
	 * If it is variable-length field, can't use this method, because the underlying data is stored continuously.
	 */
	public static boolean isFixedLength(InternalType type) {
		if (type instanceof DecimalType) {
			return ((DecimalType) type).precision() <= Decimal.MAX_COMPACT_PRECISION;
		} else {
			return MUTABLE_FIELD_TYPES.contains(type);
		}
	}

	public static boolean isMutable(InternalType type) {
		return MUTABLE_FIELD_TYPES.contains(type) || type instanceof DecimalType;
	}

	private final int arity;
	private final int nullBitsSizeInBytes;

	private MemorySegment segment;

	/**
	 * All segments that contain fixed-length part and variable-length part.
	 */
	private MemorySegment[] allSegments;
	private int baseOffset;
	private int sizeInBytes;

	public BinaryRow(int arity) {
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

	public MemorySegment getMemorySegment() {
		return segment;
	}

	public int getBaseOffset() {
		return baseOffset;
	}

	public int getSizeInBytes() {
		return sizeInBytes;
	}

	public int getFixedLengthPartSize() {
		return nullBitsSizeInBytes + 8 * arity;
	}

	public MemorySegment[] getAllSegments() {
		return allSegments;
	}

	@Override
	public int getArity() {
		return arity;
	}

	@Override
	public byte getHeader() {
		// first nullBitsSizeInBytes byte is header.
		return segment.get(baseOffset);
	}

	@Override
	public void setHeader(byte header) {
		segment.put(baseOffset, header);
	}

	public void pointTo(MemorySegment segment, int baseOffset, int sizeInBytes) {
		this.segment = segment;
		// reuse MemorySegment[].
		if (allSegments != null && allSegments.length == 1) {
			allSegments[0] = segment;
		} else {
			allSegments = new MemorySegment[] {segment};
		}
		this.baseOffset = baseOffset;
		this.sizeInBytes = sizeInBytes;
	}

	public void pointTo(MemorySegment[] segments, int baseOffset, int sizeInBytes) {
		this.segment = segments[0];
		this.allSegments = segments;
		this.baseOffset = baseOffset;
		this.sizeInBytes = sizeInBytes;
	}

	public void setTotalSize(int sizeInBytes) {
		this.sizeInBytes = sizeInBytes;
	}

	private void setNotNullAt(int i) {
		assertIndexIsValid(i);
		// need add header 8 bit.
		BitSetUtil.clear(segment, baseOffset, i + 8);
	}

	@Override
	public void setNullAt(int i) {
		assertIndexIsValid(i);
		// need add header 8 bit.
		BitSetUtil.set(segment, baseOffset, i + 8);
		// We must set the fixed length part zero.
		// 1.Only int/long/boolean...(Fix length type) will invoke this setNullAt.
		// 2.Set to zero in order to equals and hash operation bytes calculation.
		segment.putLong(getFieldOffset(i), 0);
	}

	@Override
	public void setInt(int pos, int value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		segment.putInt(getFieldOffset(pos), value);
	}

	@Override
	public void setLong(int pos, long value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		segment.putLong(getFieldOffset(pos), value);
	}

	@Override
	public void setDouble(int pos, double value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		if (Double.isNaN(value)) {
			value = Double.NaN;
		}
		segment.putDouble(getFieldOffset(pos), value);
	}

	@Override
	public void setChar(int pos, char value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		segment.putChar(getFieldOffset(pos), value);
	}

	@Override
	public void setDecimal(int pos, Decimal value, int precision, int scale) {
		assertIndexIsValid(pos);

		if (Decimal.isCompact(precision)) {
			// compact format
			setLong(pos, value.toUnscaledLong());
		} else {
			int fieldOffset = getFieldOffset(pos);
			int cursor = (int) (segment.getLong(fieldOffset) >>> 32);
			assert cursor > 0 : "invalid cursor " + cursor;
			// zero-out the bytes
			MultiSegUtil.setLong(allSegments, baseOffset + cursor, 0L);
			MultiSegUtil.setLong(allSegments, baseOffset + cursor + 8, 0L);

			if (value == null) {
				setNullAt(pos);
				// keep the offset for future update
				segment.putLong(fieldOffset, ((long) cursor) << 32);
			} else {

				byte[] bytes = value.toUnscaledBytes();
				assert(bytes.length <= 16);

				// Write the bytes to the variable length portion.
				BinaryRowUtil.copyFromBytes(
						allSegments, baseOffset + cursor, bytes, 0, bytes.length);
				setLong(pos, ((long) cursor << 32) | ((long) bytes.length));
			}
		}
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		segment.putBoolean(getFieldOffset(pos), value);
	}

	@Override
	public void setShort(int pos, short value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		segment.putShort(getFieldOffset(pos), value);
	}

	@Override
	public void setByte(int pos, byte value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		segment.put(getFieldOffset(pos), value);
	}

	@Override
	public void setFloat(int pos, float value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		if (Float.isNaN(value)) {
			value = Float.NaN;
		}
		segment.putFloat(getFieldOffset(pos), value);
	}

	@Override
	public boolean isNullAt(int pos) {
		assertIndexIsValid(pos);
		// need add header 8 bit.
		return BitSetUtil.get(segment, baseOffset, pos + 8);
	}

	@Override
	public boolean getBoolean(int pos) {
		assertIndexIsValid(pos);
		return segment.getBoolean(getFieldOffset(pos));
	}

	@Override
	public byte getByte(int pos) {
		assertIndexIsValid(pos);
		return segment.get(getFieldOffset(pos));
	}

	@Override
	public short getShort(int pos) {
		assertIndexIsValid(pos);
		return segment.getShort(getFieldOffset(pos));
	}

	@Override
	public int getInt(int pos) {
		assertIndexIsValid(pos);
		return segment.getInt(getFieldOffset(pos));
	}

	@Override
	public long getLong(int pos) {
		assertIndexIsValid(pos);
		return segment.getLong(getFieldOffset(pos));
	}

	@Override
	public float getFloat(int pos) {
		assertIndexIsValid(pos);
		return segment.getFloat(getFieldOffset(pos));
	}

	@Override
	public double getDouble(int pos) {
		assertIndexIsValid(pos);
		return segment.getDouble(getFieldOffset(pos));
	}

	@Override
	public char getChar(int pos) {
		assertIndexIsValid(pos);
		return segment.getChar(getFieldOffset(pos));
	}

	/**
	 * Write string, if length less than 8, will be include in offsetAndSize.
	 *
	 * <p>If length is less than 8, its binary format is:
	 * 1bit mark(1), 7bits length, and 7bytes data.
	 *
	 * <p>If length is greater or equal to 8, its binary format is:
	 * 4bytes offset and 4bytes length. Data is stored in variable-length part.
	 *
	 * <p>Note: Need to consider the ByteOrder.
	 */
	@Override
	public BinaryString getBinaryString(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getFieldOffset(pos);
		final long offsetAndSize = segment.getLong(fieldOffset);
		BinaryString ret = new BinaryString();
		getBinaryStringFromSeg(allSegments, baseOffset, fieldOffset, offsetAndSize, ret);
		return ret;
	}

	@Override
	public BinaryString getBinaryString(int pos, BinaryString reuse) {
		assertIndexIsValid(pos);
		int fieldOffset = getFieldOffset(pos);
		final long offsetAndSize = segment.getLong(fieldOffset);
		getBinaryStringFromSeg(allSegments, baseOffset, fieldOffset, offsetAndSize, reuse);
		return reuse;
	}

	public static void getBinaryStringFromSeg(
			MemorySegment[] segments, int baseOffset,
			int fieldOffset, long offsetAndSize, BinaryString reuse) {
		long mark = offsetAndSize & 0x8000000000000000L;
		if (mark == 0) {
			final int offset = (int) (offsetAndSize >> 32);
			final int size = (int) offsetAndSize;
			reuse.pointTo(segments, baseOffset + offset, size);
		} else {
			int size = (int) ((offsetAndSize & 0x7F00000000000000L) >>> 56);
			if (LITTLE_ENDIAN) {
				reuse.pointTo(segments, fieldOffset, size);
			} else {
				// fieldOffset + 1 to skip header.
				reuse.pointTo(segments, fieldOffset + 1, size);
			}
		}
	}

	@Override
	public String getString(int pos) {
		return getBinaryString(pos).toString();
	}

	@Override
	public Decimal getDecimal(int pos, int precision, int scale) {
		assertIndexIsValid(pos);

		if (Decimal.isCompact(precision)) {
			long longVal = segment.getLong(getFieldOffset(pos));
			return Decimal.fromUnscaledLong(precision, scale, longVal);
		}

		int fieldOffset = getFieldOffset(pos);
		final long offsetAndSize = segment.getLong(fieldOffset);
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		byte[] bytes = BinaryRowUtil.copy(allSegments, baseOffset + offset, size);
		return Decimal.fromUnscaledBytes(precision, scale, bytes);
	}

	@Override
	public <T> T getGeneric(int pos, TypeSerializer<T> serializer) {
		final long offsetAndSize = getLong(pos);
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		try {
			return InstantiationUtil.deserializeFromByteArray(
					serializer, BinaryRowUtil.copy(allSegments, baseOffset + offset, size));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public <T> T getGeneric(int ordinal, GenericType<T> type) {
		return getGeneric(ordinal, type.getSerializer());
	}

	@Override
	public BaseRow getBaseRow(int ordinal, int numFields) {
		final long offsetAndSize = getLong(ordinal);
		return getBaseRow(allSegments, baseOffset, offsetAndSize, numFields);
	}

	public static BaseRow getBaseRow(
			MemorySegment[] segments, int baseOffset, long offsetAndSize, int numFields) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		NestedRow row = new NestedRow(numFields);
		row.pointTo(segments, offset + baseOffset, size);
		return row;
	}

	@Override
	public BaseArray getBaseArray(int ordinal) {
		final long offsetAndSize = getLong(ordinal);
		return getBinaryArray(allSegments, baseOffset, offsetAndSize);
	}

	public static BinaryArray getBinaryArray(
			MemorySegment[] segments, int baseOffset, long offsetAndSize) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		BinaryArray array = new BinaryArray();
		array.pointTo(segments, offset + baseOffset, size);
		return array;
	}

	@Override
	public BaseMap getBaseMap(int ordinal) {
		final long offsetAndSize = getLong(ordinal);
		return getBinaryMap(allSegments, baseOffset, offsetAndSize);
	}

	public static BinaryMap getBinaryMap(
			MemorySegment[] segments, int baseOffset, long offsetAndSize) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		BinaryMap map = new BinaryMap();
		map.pointTo(segments, offset + baseOffset, size);
		return map;
	}

	/**
	 * Same to {@link #getBinaryString(int)}.
	 */
	@Override
	public byte[] getByteArray(int ordinal) {
		int fieldOffset = getFieldOffset(ordinal);
		final long offsetAndSize = segment.getLong(fieldOffset);
		return getByteArray(allSegments, baseOffset, fieldOffset, offsetAndSize);
	}

	public static byte[] getByteArray(
			MemorySegment[] allSegments, int baseOffset, int fieldOffset, long offsetAndSize) {
		long mark = offsetAndSize & 0x8000000000000000L;
		if (mark == 0) {
			final int offset = (int) (offsetAndSize >> 32);
			final int size = (int) offsetAndSize;
			return BinaryRowUtil.copy(allSegments, baseOffset + offset, size);
		} else {
			int size = (int) ((offsetAndSize & 0x7F00000000000000L) >>> 56);
			byte[] bytes = new byte[size];
			if (LITTLE_ENDIAN) {
				allSegments[0].get(fieldOffset, bytes, 0, size);
			} else {
				// fieldOffset + 1 to skip header.
				allSegments[0].get(fieldOffset + 1, bytes, 0, size);
			}
			return bytes;
		}
	}

	/**
	 * The bit is 1 when the field is null. Default is 0.
	 */
	public boolean anyNull() {
		for (int i = 0; i < nullBitsSizeInBytes; i += 8) {
			if (segment.getLong(i) != 0) {
				return true;
			}
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

	public BinaryRow copy() {
		return copy(new BinaryRow(arity));
	}

	public BinaryRow copy(BaseRow reuse) {
		return copyInternal((BinaryRow) reuse);
	}

	private BinaryRow copyInternal(BinaryRow reuse) {
		byte[] bytes = BinaryRowUtil.copy(allSegments, baseOffset, sizeInBytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
		return reuse;
	}

	@Override
	public String toString() {
		if (segment == null) {
			return "null";
		} else {
			StringBuilder build = new StringBuilder();
			for (int i = 0; i < getFixedLengthPartSize(); i += 8) {
				if (i != 0) {
					build.append(',');
				}
				build.append(java.lang.Long.toHexString(segment.getLong(baseOffset + i)));
			}
			return build.toString();
		}
	}

	public String toOriginString(InternalType... types) {
		return toOriginString(this, types);
	}

	public String toOriginString(BaseRow row, InternalType[] types) {
		checkArgument(types.length == row.getArity());
		StringBuilder build = new StringBuilder("[");
		build.append(row.getHeader());
		for (int i = 0; i < row.getArity(); i++) {
			build.append(',');
			if (row.isNullAt(i)) {
				build.append("null");
			} else {
				build.append(TypeGetterSetters.get(row, i, types[i]));
			}
		}
		build.append(']');
		return build.toString();
	}

	@Override
	public boolean equals(Object o) {
		return equalsFrom(o, 0);
	}

	@Override
	public boolean equalsWithoutHeader(BaseRow o) {
		return equalsFrom(o, 1);
	}

	private boolean equalsFrom(Object o, int startIndex) {
		if (o != null && o instanceof BinaryRow) {
			BinaryRow other = (BinaryRow) o;
			return sizeInBytes == other.sizeInBytes && BinaryRowUtil.equals(
				allSegments,
				baseOffset + startIndex,
				other.allSegments,
				other.baseOffset + startIndex,
				sizeInBytes - startIndex);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		if (allSegments.length == 1) {
			return Murmur32.hashBytesByWords(segment, baseOffset, sizeInBytes, DEFAULT_SEED);
		} else {
			return hashSlow();
		}
	}

	private int hashSlow() {
		byte[] bytes = new byte[sizeInBytes];
		BinaryRowUtil.copySlow(allSegments, baseOffset, bytes, sizeInBytes);
		return Murmur32.hashBytesByWords(
				MemorySegmentFactory.wrap(bytes), 0, sizeInBytes, DEFAULT_SEED);
	}

	public void unbindMemorySegment() {
		segment = null;
		allSegments = null;
		baseOffset = 0;
		sizeInBytes = 0;
	}
}
