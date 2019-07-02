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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.util.SegmentsUtil;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A special row which is backed by {@link MemorySegment} instead of Object. It can significantly
 * reduce the serialization/deserialization of Java objects.
 *
 * <p>A Row has two part: Fixed-length part and variable-length part.
 *
 * <p>Fixed-length part contains 1 byte header and null bit set and field values. Null bit set is
 * used for null tracking and is aligned to 8-byte word boundaries. `Field values` holds
 * fixed-length primitive types and variable-length values which can be stored in 8 bytes inside.
 * If it do not fit the variable-length field, then store the length and offset of variable-length
 * part.
 *
 * <p>Fixed-length part will certainly fall into a MemorySegment, which will speed up the read
 * and write of field. During the write phase, if the target memory segment has less space than
 * fixed length part size, we will skip the space. So the number of fields in a single Row cannot
 * exceed the capacity of a single MemorySegment, if there are too many fields, we suggest that
 * user set a bigger pageSize of MemorySegment.
 *
 * <p>Variable-length part may fall into multiple MemorySegments.
 *
 * <p>{@code BinaryRow} are influenced by Apache Spark UnsafeRow in project tungsten.
 * The difference is that BinaryRow is placed on a discontinuous memory, and the variable length
 * type can also be placed on a fixed length area (If it's short enough).
 */
public final class BinaryRow extends BipartiteBinaryFormat implements BaseRow {

	public static final int HEADER_SIZE_IN_BYTES = 1;

	public static final int NULL_BITS_UNIT_IN_BYTES = 8;

	public static final int FIXED_ELEMENT_SIZE_IN_BYTES = 8;

	private final int arity;

	public BinaryRow(int arity) {
		super(HEADER_SIZE_IN_BYTES, NULL_BITS_UNIT_IN_BYTES);

		checkArgument(arity >= 0);
		this.numElements = arity;
		this.arity = arity;
		this.fixedElementSizeInBytes = FIXED_ELEMENT_SIZE_IN_BYTES;

		this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(this.numElements, NULL_BITS_UNIT_IN_BYTES, HEADER_SIZE_IN_BYTES);
	}

	public static int calculateBitSetWidthInBytes(int numElements) {
		return calculateBitSetWidthInBytes(numElements, NULL_BITS_UNIT_IN_BYTES, HEADER_SIZE_IN_BYTES);
	}

	public static int calculateFixPartSizeInBytes(int numElements) {
		return calculateFixPartSizeInBytes(
			numElements, NULL_BITS_UNIT_IN_BYTES, HEADER_SIZE_IN_BYTES, FIXED_ELEMENT_SIZE_IN_BYTES);
	}

	/**
	 * If it is a fixed-length field, we can call this BinaryRow's setXX method for in-place updates.
	 * If it is variable-length field, can't use this method, because the underlying data is stored continuously.
	 */
	public static boolean isInFixedLengthPart(LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
			case BIGINT:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case INTERVAL_DAY_TIME:
			case FLOAT:
			case DOUBLE:
				return true;
			case DECIMAL:
				return ((DecimalType) type).getPrecision() <= Decimal.MAX_COMPACT_PRECISION;
			default:
				return false;
		}
	}

	public static boolean isMutable(LogicalType type) {
		return isInFixedLengthPart(type) || type.getTypeRoot() == LogicalTypeRoot.DECIMAL;
	}

	@Override
	public int getArity() {
		return arity;
	}

	@Override
	public byte getHeader() {
		// first nullBitsSizeInBytes byte is header.
		return segments[0].get(offset);
	}

	@Override
	public void setHeader(byte header) {
		segments[0].put(offset, header);
	}

	public void setTotalSize(int sizeInBytes) {
		this.sizeInBytes = sizeInBytes;
	}

	@Override
	public void setNullAt(int i) {
		super.setNullAt(i);
		// We must set the fixed length part zero.
		// 1.Only int/long/boolean...(Fix length type) will invoke this setNullAt.
		// 2.Set to zero in order to equals and hash operation bytes calculation.
		segments[0].putLong(getFieldOffset(i), 0);
	}

	public BinaryRow copy() {
		return copy(new BinaryRow(arity));
	}

	public BinaryRow copy(BinaryRow reuse) {
		return copyInternal(reuse);
	}

	private BinaryRow copyInternal(BinaryRow reuse) {
		byte[] bytes = SegmentsUtil.copyToBytes(segments, offset, sizeInBytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
		return reuse;
	}

	public String toOriginString(LogicalType... types) {
		return toOriginString(this, types);
	}

	public static String toOriginString(BaseRow row, LogicalType[] types) {
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

	public boolean equalsWithoutHeader(BaseRow o) {
		return equalsFrom(o, 1);
	}
}
