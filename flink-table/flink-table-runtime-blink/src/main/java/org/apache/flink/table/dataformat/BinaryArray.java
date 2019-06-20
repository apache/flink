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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.util.SegmentsUtil;

import static org.apache.flink.core.memory.MemoryUtils.UNSAFE;

/**
 * For fields that hold fixed-length primitive types, such as long, double, or int, we store the
 * value directly in the field, just like the original java array.
 *
 * <p>[numElements(int)] + [null bits(4-byte word boundaries)] + [values or offset&length] + [variable length part].
 *
 * <p>{@code BinaryArray} are influenced by Apache Spark UnsafeArrayData.
 */
public final class BinaryArray extends BipartiteBinaryFormat {

	/**
	 * 4 bytes for the array length and 4 bytes for the fixed data length.
	 */
	public static final int HEADER_SIZE_IN_BYTES = 8;

	public static final int NULL_BITS_UNIT_IN_BYTES = 4;

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

	/**
	 * Index for array elements in the segment.
	 */
	private int elementOffset;

	public BinaryArray() {
		super(HEADER_SIZE_IN_BYTES, NULL_BITS_UNIT_IN_BYTES);
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

	@Override
	public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
		super.pointTo(segments, offset, sizeInBytes);

		// Read the number of elements from the first 4 bytes.
		final int numElements = SegmentsUtil.getInt(segments, offset);
		assert numElements >= 0 : "numElements (" + numElements + ") should >= 0";

		final int fixedElementSizeInBytes = SegmentsUtil.getInt(segments, offset + 4);
		assert numElements >= 0 : "fixedElementSizeInBytes (" + fixedElementSizeInBytes + ") should >= 0";

		this.numElements = numElements;
		this.fixedElementSizeInBytes = fixedElementSizeInBytes;
		this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(this.numElements, NULL_BITS_UNIT_IN_BYTES, HEADER_SIZE_IN_BYTES);
		this.elementOffset = offset + nullBitsSizeInBytes;
	}

	private void checkNoNull() {
		if (anyNull()) {
			throw new RuntimeException("Array can not have null value!");
		}
	}

	public boolean[] toBooleanArray() {
		checkNoNull();
		boolean[] values = new boolean[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, BOOLEAN_ARRAY_OFFSET, numElements);
		return values;
	}

	public byte[] toByteArray() {
		checkNoNull();
		byte[] values = new byte[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, BYTE_ARRAY_BASE_OFFSET, numElements);
		return values;
	}

	public short[] toShortArray() {
		checkNoNull();
		short[] values = new short[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, SHORT_ARRAY_OFFSET, numElements * 2);
		return values;
	}

	public int[] toIntArray() {
		checkNoNull();
		int[] values = new int[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, INT_ARRAY_OFFSET, numElements * 4);
		return values;
	}

	public long[] toLongArray() {
		checkNoNull();
		long[] values = new long[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, LONG_ARRAY_OFFSET, numElements * 8);
		return values;
	}

	public float[] toFloatArray() {
		checkNoNull();
		float[] values = new float[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, FLOAT_ARRAY_OFFSET, numElements * 4);
		return values;
	}

	public double[] toDoubleArray() {
		checkNoNull();
		double[] values = new double[numElements];
		SegmentsUtil.copyToUnsafe(
				segments, elementOffset, values, DOUBLE_ARRAY_OFFSET, numElements * 8);
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

	private static BinaryArray fromPrimitiveArray(
			Object arr, int offset, int length, int elementSize) {
		final long headerInBytes = calculateBitSetWidthInBytes(length, NULL_BITS_UNIT_IN_BYTES, HEADER_SIZE_IN_BYTES);
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
		UNSAFE.putInt(data, (long) BYTE_ARRAY_BASE_OFFSET + 4, elementSize);
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

	public void setNullBoolean(int pos) {
		super.setNullAt(pos);
		SegmentsUtil.setBoolean(segments, getFieldOffset(pos), false);
	}

	public void setNullByte(int pos) {
		super.setNullAt(pos);
		SegmentsUtil.setByte(segments, getFieldOffset(pos), (byte) 0);
	}

	public void setNullShort(int pos) {
		super.setNullAt(pos);
		SegmentsUtil.setShort(segments, getFieldOffset(pos), (short) 0);
	}

	public void setNullInt(int pos) {
		super.setNullAt(pos);
		SegmentsUtil.setInt(segments, getFieldOffset(pos), 0);
	}

	public void setNullLong(int pos) {
		super.setNullAt(pos);
		SegmentsUtil.setLong(segments, getFieldOffset(pos), 0L);
	}

	public void setNullFloat(int pos) {
		setNullInt(pos);
	}

	public void setNullDouble(int pos) {
		setNullLong(pos);
	}
}

