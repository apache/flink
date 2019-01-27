/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.Types;
import org.apache.flink.table.dataformat.util.BinaryRowUtil;
import org.apache.flink.table.dataformat.util.MultiSegUtil;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Array;

import static org.apache.flink.table.dataformat.BinaryRow.getBinaryStringFromSeg;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.BOOLEAN_ARRAY_OFFSET;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.BYTE_ARRAY_BASE_OFFSET;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.DOUBLE_ARRAY_OFFSET;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.FLOAT_ARRAY_OFFSET;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.INT_ARRAY_OFFSET;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.LONG_ARRAY_OFFSET;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.SHORT_ARRAY_OFFSET;
import static org.apache.flink.table.dataformat.util.BinaryRowUtil.UNSAFE;

/**
 * For fields that hold fixed-length primitive types, such as long, double, or int, we store the
 * value directly in the field, just like the original java array.
 *
 * <p>[numElements(int)] + [null bits(4-byte word boundaries)] + [values or offset&length] + [variable length part].
 *
 * <p>{@code BinaryArray} are influenced by Apache Spark UnsafeArrayData.
 */
public class BinaryArray extends BaseArray {

	public static int calculateHeaderInBytes(int numFields) {
		return 4 + ((numFields + 31) / 32) * 4;
	}

	public static int calculateElementSize(InternalType type) {
		if (type.equals(Types.BOOLEAN)) {
			return 1;
		} else if (type.equals(Types.BYTE)) {
			return 1;
		} else if (type.equals(Types.SHORT)) {
			return 2;
		} else if (type.equals(Types.INT)) {
			return 4;
		} else if (type.equals(Types.FLOAT)) {
			return 4;
		} else if (type.equals(Types.CHAR)) {
			return 2;
		} else if (type.equals(Types.DATE)) {
			return 4;
		} else if (type.equals(Types.TIME)) {
			return 4;
		} else {
			return 8;
		}
	}

	private MemorySegment[] segments;
	private int baseOffset;

	// The number of elements in this array
	private int numElements;

	// The size of this array's backing data, in bytes.
	// Header is also included.
	private int sizeInBytes;

	/** The position to start storing array elements. */
	private int elementOffset;

	public BinaryArray() {}

	private void assertIndexIsValid(int ordinal) {
		assert ordinal >= 0 : "ordinal (" + ordinal + ") should >= 0";
		assert ordinal < numElements : "ordinal (" + ordinal + ") should < " + numElements;
	}

	private int getElementOffset(int ordinal, int elementSize) {
		return elementOffset + ordinal * elementSize;
	}

	public MemorySegment[] getSegments() {
		return segments;
	}

	public int getBaseOffset() {
		return baseOffset;
	}

	public int getSizeInBytes() {
		return sizeInBytes;
	}

	@Override
	public int numElements() {
		return numElements;
	}

	public void setTotalSize(int sizeInBytes) {
		this.sizeInBytes = sizeInBytes;
	}

	public void pointTo(MemorySegment segment, int baseOffset, int sizeInBytes) {
		pointTo(new MemorySegment[]{segment}, baseOffset, sizeInBytes);
	}

	public void pointTo(MemorySegment[] segments, int baseOffset, int sizeInBytes) {
		// Read the number of elements from the first 4 bytes.
		final int numElements = MultiSegUtil.getInt(segments, baseOffset);
		assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";

		this.numElements = numElements;
		this.segments = segments;
		this.baseOffset = baseOffset;
		this.sizeInBytes = sizeInBytes;
		this.elementOffset = baseOffset + calculateHeaderInBytes(this.numElements);
	}

	@Override
	public boolean isNullAt(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.bitGet(segments, baseOffset + 4, pos);
	}

	@Override
	public void setNullAt(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
	}

	@Override
	public void setNotNullAt(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitUnSet(segments, baseOffset + 4, pos);
	}

	@Override
	public long getLong(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getLong(segments, getElementOffset(pos, 8));
	}

	@Override
	public void setLong(int pos, long value) {
		assertIndexIsValid(pos);
		MultiSegUtil.setLong(segments, getElementOffset(pos, 8), value);
	}

	@Override
	public void setNullLong(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
		MultiSegUtil.setLong(segments, getElementOffset(pos, 8), 0L);
	}

	@Override
	public int getInt(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getInt(segments, getElementOffset(pos, 4));
	}

	@Override
	public void setInt(int pos, int value) {
		assertIndexIsValid(pos);
		MultiSegUtil.setInt(segments, getElementOffset(pos, 4), value);
	}

	@Override
	public void setNullInt(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
		MultiSegUtil.setInt(segments, getElementOffset(pos, 4), 0);
	}

	@Override
	public BinaryString getBinaryString(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getElementOffset(pos, 8);
		final long offsetAndSize = MultiSegUtil.getLong(segments, fieldOffset);
		BinaryString ret = new BinaryString();
		getBinaryStringFromSeg(segments, baseOffset, fieldOffset, offsetAndSize, ret);
		return ret;
	}

	@Override
	public BinaryString getBinaryString(int pos, BinaryString reuse) {
		assertIndexIsValid(pos);
		int fieldOffset = getElementOffset(pos, 8);
		final long offsetAndSize = MultiSegUtil.getLong(segments, fieldOffset);
		getBinaryStringFromSeg(segments, baseOffset, fieldOffset, offsetAndSize, reuse);
		return reuse;
	}

	@Override
	public Decimal getDecimal(int pos, int precision, int scale) {
		assertIndexIsValid(pos);

		if (Decimal.isCompact(precision)) {
			long longVal = MultiSegUtil.getLong(segments, getElementOffset(pos, 8));
			return Decimal.fromUnscaledLong(precision, scale, longVal);
		}

		int fieldOffset = getElementOffset(pos, 8);
		final long offsetAndSize = MultiSegUtil.getLong(segments, fieldOffset);
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		byte[] bytes = BinaryRowUtil.copy(segments, baseOffset + offset, size);
		return Decimal.fromUnscaledBytes(precision, scale, bytes);
	}

	@Override
	public boolean getBoolean(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getBoolean(segments, getElementOffset(pos, 1));
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		assertIndexIsValid(pos);
		MultiSegUtil.setBoolean(segments, getElementOffset(pos, 1), value);
	}

	@Override
	public void setNullBoolean(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
		MultiSegUtil.setBoolean(segments, getElementOffset(pos, 1), false);
	}

	@Override
	public byte getByte(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getByte(segments, getElementOffset(pos, 1));
	}

	@Override
	public void setByte(int pos, byte value) {
		assertIndexIsValid(pos);
		MultiSegUtil.setByte(segments, getElementOffset(pos, 1), value);
	}

	@Override
	public void setNullByte(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
		MultiSegUtil.setByte(segments, getElementOffset(pos, 1), (byte) 0);
	}

	@Override
	public short getShort(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getShort(segments, getElementOffset(pos, 2));
	}

	@Override
	public void setShort(int pos, short value) {
		assertIndexIsValid(pos);
		MultiSegUtil.setShort(segments, getElementOffset(pos, 2), value);
	}

	@Override
	public void setNullShort(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
		MultiSegUtil.setShort(segments, getElementOffset(pos, 2), (short) 0);
	}

	@Override
	public float getFloat(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getFloat(segments, getElementOffset(pos, 4));
	}

	@Override
	public void setFloat(int pos, float value) {
		assertIndexIsValid(pos);
		MultiSegUtil.setFloat(segments, getElementOffset(pos, 4), value);
	}

	@Override
	public void setNullFloat(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
		MultiSegUtil.setFloat(segments, getElementOffset(pos, 4), 0F);
	}

	@Override
	public double getDouble(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getDouble(segments, getElementOffset(pos, 8));
	}

	@Override
	public void setDouble(int pos, double value) {
		assertIndexIsValid(pos);
		MultiSegUtil.setDouble(segments, getElementOffset(pos, 8), value);
	}

	@Override
	public void setNullDouble(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
		MultiSegUtil.setDouble(segments, getElementOffset(pos, 8), 0.0);
	}

	@Override
	public char getChar(int pos) {
		assertIndexIsValid(pos);
		return MultiSegUtil.getChar(segments, getElementOffset(pos, 2));
	}

	@Override
	public void setChar(int pos, char value) {
		assertIndexIsValid(pos);
		MultiSegUtil.setChar(segments, getElementOffset(pos, 2), value);
	}

	@Override
	public void setNullChar(int pos) {
		assertIndexIsValid(pos);
		MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
		MultiSegUtil.setChar(segments, getElementOffset(pos, 2), '\0');
	}

	@Override
	public void setDecimal(int pos, Decimal value, int precision, int scale) {
		assertIndexIsValid(pos);

		if (Decimal.isCompact(precision)) {
			// compact format
			if (value == null) {
				setNullLong(pos);
			} else {
				setLong(pos, value.toUnscaledLong());
			}
		} else {
			int fieldOffset = getElementOffset(pos, 8);
			int cursor = (int) (MultiSegUtil.getLong(segments, fieldOffset) >>> 32);
			assert cursor > 0 : "invalid cursor " + cursor;
			// zero-out the bytes
			MultiSegUtil.setLong(segments, baseOffset + cursor, 0L);
			MultiSegUtil.setLong(segments, baseOffset + cursor + 8, 0L);

			if (value == null) {
				MultiSegUtil.bitSet(segments, baseOffset + 4, pos);
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

	public BinaryArray copy() {
		return copy(new BinaryArray());
	}

	public BinaryArray copy(BinaryArray reuse) {
		byte[] bytes = BinaryRowUtil.copy(segments, baseOffset, sizeInBytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
		return reuse;
	}

	public byte[] getBytes() {
		return MultiSegUtil.getBytes(segments, baseOffset, sizeInBytes);
	}

	@Override
	public boolean[] toBooleanArray() {
		boolean[] values = new boolean[numElements];
		BinaryRowUtil.copyToUnsafe(
				segments, elementOffset, values, BOOLEAN_ARRAY_OFFSET, numElements);
		return values;
	}

	@Override
	public byte[] toByteArray() {
		byte[] values = new byte[numElements];
		BinaryRowUtil.copyToUnsafe(
				segments, elementOffset, values, BYTE_ARRAY_BASE_OFFSET, numElements);
		return values;
	}

	@Override
	public short[] toShortArray() {
		short[] values = new short[numElements];
		BinaryRowUtil.copyToUnsafe(
				segments, elementOffset, values, SHORT_ARRAY_OFFSET, numElements * 2);
		return values;
	}

	@Override
	public int[] toIntArray() {
		int[] values = new int[numElements];
		BinaryRowUtil.copyToUnsafe(
				segments, elementOffset, values, INT_ARRAY_OFFSET, numElements * 4);
		return values;
	}

	@Override
	public long[] toLongArray() {
		long[] values = new long[numElements];
		BinaryRowUtil.copyToUnsafe(
				segments, elementOffset, values, LONG_ARRAY_OFFSET, numElements * 8);
		return values;
	}

	@Override
	public float[] toFloatArray() {
		float[] values = new float[numElements];
		BinaryRowUtil.copyToUnsafe(
				segments, elementOffset, values, FLOAT_ARRAY_OFFSET, numElements * 4);
		return values;
	}

	@Override
	public double[] toDoubleArray() {
		double[] values = new double[numElements];
		BinaryRowUtil.copyToUnsafe(
				segments, elementOffset, values, DOUBLE_ARRAY_OFFSET, numElements * 8);
		return values;
	}

	private static BinaryArray fromPrimitiveArray(
			Object arr, int offset, int length, int elementSize) {
		final long headerInBytes = calculateHeaderInBytes(length);
		final long valueRegionInBytes = elementSize * length;
		final long totalSize = headerInBytes + valueRegionInBytes;
		if (totalSize > Integer.MAX_VALUE) {
			throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
					"it's too big.");
		}

		final byte[] data = new byte[(int) totalSize];

		UNSAFE.putInt(data, BYTE_ARRAY_BASE_OFFSET, length);
		UNSAFE.copyMemory(
				arr, offset, data, BYTE_ARRAY_BASE_OFFSET + headerInBytes, valueRegionInBytes);

		BinaryArray result = new BinaryArray();
		result.pointTo(MemorySegmentFactory.wrap(data), 0, (int) totalSize);
		return result;
	}

	public static BinaryArray fromPrimitiveArray(boolean[] arr) {
		return fromPrimitiveArray(arr, BOOLEAN_ARRAY_OFFSET, arr.length, 1);
	}

	public static BinaryArray fromPrimitiveArray(byte[] arr) {
		return fromPrimitiveArray(arr, BYTE_ARRAY_BASE_OFFSET, arr.length, 1);
	}

	public static BinaryArray fromPrimitiveArray(short[] arr) {
		return fromPrimitiveArray(arr, SHORT_ARRAY_OFFSET, arr.length, 2);
	}

	public static BinaryArray fromPrimitiveArray(int[] arr) {
		return fromPrimitiveArray(arr, INT_ARRAY_OFFSET, arr.length, 4);
	}

	public static BinaryArray fromPrimitiveArray(long[] arr) {
		return fromPrimitiveArray(arr, LONG_ARRAY_OFFSET, arr.length, 8);
	}

	public static BinaryArray fromPrimitiveArray(float[] arr) {
		return fromPrimitiveArray(arr, FLOAT_ARRAY_OFFSET, arr.length, 4);
	}

	public static BinaryArray fromPrimitiveArray(double[] arr) {
		return fromPrimitiveArray(arr, DOUBLE_ARRAY_OFFSET, arr.length, 8);
	}

	@Override
	public Object[] toObjectArray(InternalType elementType) {
		int size = numElements();
		Object[] values = new Object[size];
		for (int i = 0; i < size; i++) {
			if (isNullAt(i)) {
				values[i] = null;
			} else {
				values[i] = TypeGetterSetters.get(this, i, elementType);
			}
		}
		return values;
	}

	@Override
	public <T> T[] toClassArray(InternalType elementType, Class<T> elementClass) {
		int size = numElements();
		T[] values = (T[]) Array.newInstance(elementClass, size);
		for (int i = 0; i < size; i++) {
			if (!isNullAt(i)) {
				values[i] = (T) TypeGetterSetters.get(this, i, elementType);
			}
		}
		return values;
	}

	@Override
	public byte[] getByteArray(int ordinal) {
		int fieldOffset = getElementOffset(ordinal, 8);
		final long offsetAndSize = MultiSegUtil.getLong(segments, fieldOffset);
		return BinaryRow.getByteArray(segments, baseOffset, fieldOffset, offsetAndSize);
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
	public <T> T getGeneric(int pos, GenericType<T> genericType) {
		return getGeneric(pos, genericType.getSerializer());
	}

	@Override
	public <T> T getGeneric(int pos, TypeSerializer<T> serializer) {
		final long offsetAndSize = getLong(pos);
		final int size = ((int) offsetAndSize);
		final int offset = (int) (offsetAndSize >> 32);
		try {
			return InstantiationUtil.deserializeFromByteArray(
					serializer, BinaryRowUtil.copy(segments, baseOffset + offset, size));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BinaryArray) {
			BinaryArray other = (BinaryArray) o;
			return sizeInBytes == other.sizeInBytes && BinaryRowUtil.equals(
					segments,
					baseOffset,
					other.segments,
					other.baseOffset,
					sizeInBytes);
		} else {
			return false;
		}
	}
}
