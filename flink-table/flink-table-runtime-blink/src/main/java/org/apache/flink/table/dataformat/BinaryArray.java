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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.runtime.util.SegmentsUtil;
import org.apache.flink.table.types.logical.LogicalType;

import java.lang.reflect.Array;

import static org.apache.flink.core.memory.MemoryUtils.UNSAFE;

/**
 * For fields that hold fixed-length primitive types, such as long, double, or int, we store the
 * value directly in the field, just like the original java array.
 *
 * <p>[numElements(int)] + [null bits(4-byte word boundaries)] + [values or offset&length] + [variable length part].
 *
 * <p>{@code BinaryArray} are influenced by Apache Spark UnsafeArrayData.
 */
public final class BinaryArray extends BinarySection implements BaseArray {

	/**
	 * Offset for Arrays.
	 */
	private static final int BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
	private static final int BOOLEAN_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(boolean[].class);
	private static final int SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
	private static final int INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
	private static final int LONG_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(long[].class);
	private static final int FLOAT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(float[].class);
	private static final int DOUBLE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(double[].class);

	public static int calculateHeaderInBytes(int numFields) {
		return 4 + ((numFields + 31) / 32) * 4;
	}

	/**
	 * It store real value when type is primitive.
	 * It store the length and offset of variable-length part when type is string, map, etc.
	 */
	public static int calculateFixLengthPartSize(LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
			case TINYINT:
				return 1;
			case SMALLINT:
				return 2;
			case INTEGER:
			case FLOAT:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return 4;
			default:
				// long, double is 8 bytes.
				// It store the length and offset of variable-length part when type is string, map, etc.
				return 8;
		}
	}

	// The number of elements in this array
	private int numElements;

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

	@Override
	public int numElements() {
		return numElements;
	}

	@Override
	public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
		// Read the number of elements from the first 4 bytes.
		final int numElements = SegmentsUtil.getInt(segments, offset);
		assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";

		this.numElements = numElements;
		this.segments = segments;
		this.offset = offset;
		this.sizeInBytes = sizeInBytes;
		this.elementOffset = offset + calculateHeaderInBytes(this.numElements);
	}

	@Override
	public boolean isNullAt(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.bitGet(segments, offset + 4, pos);
	}

	@Override
	public void setNullAt(int pos) {
		assertIndexIsValid(pos);
		SegmentsUtil.bitSet(segments, offset + 4, pos);
	}

	@Override
	public void setNotNullAt(int pos) {
		assertIndexIsValid(pos);
		SegmentsUtil.bitUnSet(segments, offset + 4, pos);
	}

	@Override
	public long getLong(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getLong(segments, getElementOffset(pos, 8));
	}

	@Override
	public void setLong(int pos, long value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setLong(segments, getElementOffset(pos, 8), value);
	}

	@Override
	public void setNullLong(int pos) {
		assertIndexIsValid(pos);
		SegmentsUtil.bitSet(segments, offset + 4, pos);
		SegmentsUtil.setLong(segments, getElementOffset(pos, 8), 0L);
	}

	@Override
	public int getInt(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getInt(segments, getElementOffset(pos, 4));
	}

	@Override
	public void setInt(int pos, int value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setInt(segments, getElementOffset(pos, 4), value);
	}

	@Override
	public void setNullInt(int pos) {
		assertIndexIsValid(pos);
		SegmentsUtil.bitSet(segments, offset + 4, pos);
		SegmentsUtil.setInt(segments, getElementOffset(pos, 4), 0);
	}

	@Override
	public BinaryString getString(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getElementOffset(pos, 8);
		final long offsetAndSize = SegmentsUtil.getLong(segments, fieldOffset);
		return BinaryFormat.readBinaryStringFieldFromSegments(
				segments, offset, fieldOffset, offsetAndSize);
	}

	@Override
	public Decimal getDecimal(int pos, int precision, int scale) {
		assertIndexIsValid(pos);
		if (Decimal.isCompact(precision)) {
			return Decimal.fromUnscaledLong(precision, scale,
					SegmentsUtil.getLong(segments, getElementOffset(pos, 8)));
		}

		int fieldOffset = getElementOffset(pos, 8);
		final long offsetAndSize = SegmentsUtil.getLong(segments, fieldOffset);
		return Decimal.readDecimalFieldFromSegments(segments, offset, offsetAndSize, precision, scale);
	}

	@Override
	public <T> BinaryGeneric<T> getGeneric(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getElementOffset(pos, 8);
		final long offsetAndSize = SegmentsUtil.getLong(segments, fieldOffset);
		return BinaryGeneric.readBinaryGenericFieldFromSegments(
				segments, offset, offsetAndSize);
	}

	@Override
	public byte[] getBinary(int pos) {
		assertIndexIsValid(pos);
		int fieldOffset = getElementOffset(pos, 8);
		final long offsetAndSize = SegmentsUtil.getLong(segments, fieldOffset);
		return BinaryFormat.readBinaryFieldFromSegments(
				segments, offset, fieldOffset, offsetAndSize);
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

	@Override
	public BaseRow getRow(int pos, int numFields) {
		assertIndexIsValid(pos);
		int fieldOffset = getElementOffset(pos, 8);
		final long offsetAndSize = SegmentsUtil.getLong(segments, fieldOffset);
		return NestedRow.readNestedRowFieldFromSegments(
				segments, numFields, offset, offsetAndSize);
	}

	@Override
	public boolean getBoolean(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getBoolean(segments, getElementOffset(pos, 1));
	}

	@Override
	public void setBoolean(int pos, boolean value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setBoolean(segments, getElementOffset(pos, 1), value);
	}

	@Override
	public void setNullBoolean(int pos) {
		assertIndexIsValid(pos);
		SegmentsUtil.bitSet(segments, offset + 4, pos);
		SegmentsUtil.setBoolean(segments, getElementOffset(pos, 1), false);
	}

	@Override
	public byte getByte(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getByte(segments, getElementOffset(pos, 1));
	}

	@Override
	public void setByte(int pos, byte value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setByte(segments, getElementOffset(pos, 1), value);
	}

	@Override
	public void setNullByte(int pos) {
		assertIndexIsValid(pos);
		SegmentsUtil.bitSet(segments, offset + 4, pos);
		SegmentsUtil.setByte(segments, getElementOffset(pos, 1), (byte) 0);
	}

	@Override
	public short getShort(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getShort(segments, getElementOffset(pos, 2));
	}

	@Override
	public void setShort(int pos, short value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setShort(segments, getElementOffset(pos, 2), value);
	}

	@Override
	public void setNullShort(int pos) {
		assertIndexIsValid(pos);
		SegmentsUtil.bitSet(segments, offset + 4, pos);
		SegmentsUtil.setShort(segments, getElementOffset(pos, 2), (short) 0);
	}

	@Override
	public float getFloat(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getFloat(segments, getElementOffset(pos, 4));
	}

	@Override
	public void setFloat(int pos, float value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setFloat(segments, getElementOffset(pos, 4), value);
	}

	@Override
	public void setNullFloat(int pos) {
		assertIndexIsValid(pos);
		SegmentsUtil.bitSet(segments, offset + 4, pos);
		SegmentsUtil.setFloat(segments, getElementOffset(pos, 4), 0F);
	}

	@Override
	public double getDouble(int pos) {
		assertIndexIsValid(pos);
		return SegmentsUtil.getDouble(segments, getElementOffset(pos, 8));
	}

	@Override
	public void setDouble(int pos, double value) {
		assertIndexIsValid(pos);
		setNotNullAt(pos);
		SegmentsUtil.setDouble(segments, getElementOffset(pos, 8), value);
	}

	@Override
	public void setNullDouble(int pos) {
		assertIndexIsValid(pos);
		SegmentsUtil.bitSet(segments, offset + 4, pos);
		SegmentsUtil.setDouble(segments, getElementOffset(pos, 8), 0.0);
	}

	@Override
	public void setDecimal(int pos, Decimal value, int precision) {
		assertIndexIsValid(pos);

		if (Decimal.isCompact(precision)) {
			// compact format
			setLong(pos, value.toUnscaledLong());
		} else {
			int fieldOffset = getElementOffset(pos, 8);
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

	public boolean anyNull() {
		for (int i = offset + 4; i < elementOffset; i += 4) {
			if (SegmentsUtil.getInt(segments, i) != 0) {
				return true;
			}
		}
		return false;
	}

	private void checkNoNull() {
		if (anyNull()) {
			throw new RuntimeException("Array can not have null value!");
		}
	}

	@Override
	public boolean[] toBooleanArray() {
		checkNoNull();
		boolean[] values = new boolean[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, BOOLEAN_ARRAY_OFFSET, numElements);
		return values;
	}

	@Override
	public byte[] toByteArray() {
		checkNoNull();
		byte[] values = new byte[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, BYTE_ARRAY_BASE_OFFSET, numElements);
		return values;
	}

	@Override
	public short[] toShortArray() {
		checkNoNull();
		short[] values = new short[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, SHORT_ARRAY_OFFSET, numElements * 2);
		return values;
	}

	@Override
	public int[] toIntArray() {
		checkNoNull();
		int[] values = new int[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, INT_ARRAY_OFFSET, numElements * 4);
		return values;
	}

	@Override
	public long[] toLongArray() {
		checkNoNull();
		long[] values = new long[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, LONG_ARRAY_OFFSET, numElements * 8);
		return values;
	}

	@Override
	public float[] toFloatArray() {
		checkNoNull();
		float[] values = new float[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, FLOAT_ARRAY_OFFSET, numElements * 4);
		return values;
	}

	@Override
	public double[] toDoubleArray() {
		checkNoNull();
		double[] values = new double[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, DOUBLE_ARRAY_OFFSET, numElements * 8);
		return values;
	}

	public <T> T[] toClassArray(LogicalType elementType, Class<T> elementClass) {
		int size = numElements();
		T[] values = (T[]) Array.newInstance(elementClass, size);
		for (int i = 0; i < size; i++) {
			if (!isNullAt(i)) {
				values[i] = (T) TypeGetterSetters.get(this, i, elementType);
			}
		}
		return values;
	}

	public BinaryArray copy() {
		return copy(new BinaryArray());
	}

	public BinaryArray copy(BinaryArray reuse) {
		byte[] bytes = SegmentsUtil.copyToBytes(segments, offset, sizeInBytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
		return reuse;
	}

	@Override
	public int hashCode() {
		return SegmentsUtil.hashByWords(segments, offset, sizeInBytes);
	}

	private static BinaryArray fromPrimitiveArray(
			Object arr, int offset, int length, int elementSize) {
		final long headerInBytes = calculateHeaderInBytes(length);
		final long valueRegionInBytes = elementSize * length;

		// must align by 8 bytes
		long totalSizeInLongs = (headerInBytes + valueRegionInBytes + 7) / 8;
		if (totalSizeInLongs > Integer.MAX_VALUE / 8) {
			throw new UnsupportedOperationException("Cannot convert this array to unsafe format as " +
					"it's too big.");
		}
		long totalSize = totalSizeInLongs * 8;

		final byte[] data = new byte[(int) totalSize];

		UNSAFE.putInt(data, (long) BYTE_ARRAY_BASE_OFFSET, length);
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

	static BinaryArray readBinaryArrayFieldFromSegments(
			MemorySegment[] segments, int baseOffset, long offsetAndSize) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		BinaryArray array = new BinaryArray();
		array.pointTo(segments, offset + baseOffset, size);
		return array;
	}
}
