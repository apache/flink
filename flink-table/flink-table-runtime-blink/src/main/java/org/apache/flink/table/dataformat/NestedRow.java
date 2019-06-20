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
import org.apache.flink.table.util.SegmentsUtil;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Its memory storage structure and {@link BinaryRow} exactly the same, the only different is it supports
 * all bytes in variable MemorySegments.
 */
public final class NestedRow extends BipartiteBinaryFormat implements BaseRow {

	private final int arity;

	public NestedRow(int arity) {
		super(BinaryRow.HEADER_SIZE_IN_BYTES, BinaryRow.NULL_BITS_UNIT_IN_BYTES);

		checkArgument(arity >= 0);
		this.arity = arity;
		this.numElements = arity;
		this.fixedElementSizeInBytes = BinaryRow.FIXED_ELEMENT_SIZE_IN_BYTES;

		this.nullBitsSizeInBytes = calculateBitSetWidthInBytes(arity,
			BinaryRow.NULL_BITS_UNIT_IN_BYTES, BinaryRow.HEADER_SIZE_IN_BYTES);
	}

	static NestedRow readNestedRowFieldFromSegments(
			MemorySegment[] segments, int numFields, int baseOffset, long offsetAndSize) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		NestedRow row = new NestedRow(numFields);
		row.pointTo(segments, offset + baseOffset, size);
		return row;
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

	/**
	 * See {@link BinaryRow#setNullAt(int)}.
	 */
	@Override
	public void setNullAt(int i) {
		super.setNullAt(i);
		SegmentsUtil.setLong(segments, getFieldOffset(i), 0);
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
}
