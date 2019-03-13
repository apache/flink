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

/**
 * Binary format that in {@link MemorySegment}s.
 */
public abstract class BinaryFormat {

	/**
	 * It decides whether to put data in FixLenPart or VarLenPart. See more in {@link BinaryRow}.
	 *
	 * <p>If len is less than 8, its binary format is:
	 * 1bit mark(1) = 1, 7bits len, and 7bytes data.
	 * Data is stored in fix-length part.
	 *
	 * <p>If len is greater or equal to 8, its binary format is:
	 * 1bit mark(1) = 0, 31bits offset, and 4bytes len.
	 * Data is stored in variable-length part.
	 */
	static final int MAX_FIX_PART_DATA_SIZE = 7;

	/**
	 * To get the mark in highest first bit.
	 * Form: 1000 0000 0000 0000 ...
	 */
	private static final long HIGHEST_FIRST_BIT = Long.MIN_VALUE;

	/**
	 * To get the 7 bits length in second bit to eighth bit.
	 * Form: 0111 1111 0000 0000 ...
	 */
	private static final long HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;

	protected MemorySegment[] segments;
	protected int offset;
	protected int sizeInBytes;

	public BinaryFormat() {}

	public BinaryFormat(MemorySegment[] segments, int offset, int sizeInBytes) {
		this.segments = segments;
		this.offset = offset;
		this.sizeInBytes = sizeInBytes;
	}

	public final void pointTo(MemorySegment segment, int offset, int sizeInBytes) {
		pointTo(new MemorySegment[] {segment}, offset, sizeInBytes);
	}

	public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
		this.segments = segments;
		this.offset = offset;
		this.sizeInBytes = sizeInBytes;
	}

	public MemorySegment[] getSegments() {
		return segments;
	}

	public int getOffset() {
		return offset;
	}

	public int getSizeInBytes() {
		return sizeInBytes;
	}

	@Override
	public boolean equals(Object o) {
		return this == o || o != null &&
				getClass() == o.getClass() &&
				binaryEquals((BinaryFormat) o);
	}

	protected boolean binaryEquals(BinaryFormat that) {
		return sizeInBytes == that.sizeInBytes &&
				SegmentsUtil.equals(segments, offset, that.segments, that.offset, sizeInBytes);
	}

	@Override
	public int hashCode() {
		return SegmentsUtil.hash(segments, offset, sizeInBytes);
	}

	/**
	 * Get binary, if len less than 8, will be include in variablePartOffsetAndLen.
	 *
	 * <p>Note: Need to consider the ByteOrder.
	 *
	 * @param baseOffset base offset of composite binary format.
	 * @param fieldOffset absolute start offset of 'variablePartOffsetAndLen'.
	 * @param variablePartOffsetAndLen a long value, real data or offset and len.
	 */
	static byte[] readBinaryFieldFromSegments(
			MemorySegment[] segments, int baseOffset, int fieldOffset,
			long variablePartOffsetAndLen) {
		long mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
		if (mark == 0) {
			final int subOffset = (int) (variablePartOffsetAndLen >> 32);
			final int len = (int) variablePartOffsetAndLen;
			return SegmentsUtil.copyToBytes(segments, baseOffset + subOffset, len);
		} else {
			int len = (int) ((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >>> 56);
			if (SegmentsUtil.LITTLE_ENDIAN) {
				return SegmentsUtil.copyToBytes(segments, fieldOffset, len);
			} else {
				// fieldOffset + 1 to skip header.
				return SegmentsUtil.copyToBytes(segments, fieldOffset + 1, len);
			}
		}
	}

	/**
	 * Get binary string, if len less than 8, will be include in variablePartOffsetAndLen.
	 *
	 * <p>Note: Need to consider the ByteOrder.
	 *
	 * @param baseOffset base offset of composite binary format.
	 * @param fieldOffset absolute start offset of 'variablePartOffsetAndLen'.
	 * @param variablePartOffsetAndLen a long value, real data or offset and len.
	 */
	static BinaryString readBinaryStringFieldFromSegments(
			MemorySegment[] segments, int baseOffset, int fieldOffset,
			long variablePartOffsetAndLen) {
		long mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
		if (mark == 0) {
			final int subOffset = (int) (variablePartOffsetAndLen >> 32);
			final int len = (int) variablePartOffsetAndLen;
			return new BinaryString(segments, baseOffset + subOffset, len);
		} else {
			int len = (int) ((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >>> 56);
			if (SegmentsUtil.LITTLE_ENDIAN) {
				return new BinaryString(segments, fieldOffset, len);
			} else {
				// fieldOffset + 1 to skip header.
				return new BinaryString(segments, fieldOffset + 1, len);
			}
		}
	}
}
