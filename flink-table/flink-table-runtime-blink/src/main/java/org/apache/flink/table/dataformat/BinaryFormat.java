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
import org.apache.flink.table.runtime.util.SegmentsUtil;

/**
 * Binary format spanning {@link MemorySegment}s.
 */
public interface BinaryFormat {
	/**
	 * It decides whether to put data in FixLenPart or VarLenPart. See more in {@link BinaryRow}.
	 *
	 * <p>If len is less than 8, its binary format is:
	 * 1-bit mark(1) = 1, 7-bits len, and 7-bytes data.
	 * Data is stored in fix-length part.
	 *
	 * <p>If len is greater or equal to 8, its binary format is:
	 * 1-bit mark(1) = 0, 31-bits offset to the data, and 4-bytes length of data.
	 * Data is stored in variable-length part.
	 */
	int MAX_FIX_PART_DATA_SIZE = 7;
	/**
	 * To get the mark in highest bit of long.
	 * Form: 10000000 00000000 ... (8 bytes)
	 *
	 * <p>This is used to decide whether the data is stored in fixed-length part or variable-length
	 * part. see {@link #MAX_FIX_PART_DATA_SIZE} for more information.
	 */
	long HIGHEST_FIRST_BIT = 0x80L << 56;
	/**
	 * To get the 7 bits length in second bit to eighth bit out of a long.
	 * Form: 01111111 00000000 ... (8 bytes)
	 *
	 * <p>This is used to get the length of the data which is stored in this long.
	 * see {@link #MAX_FIX_PART_DATA_SIZE} for more information.
	 */
	long HIGHEST_SECOND_TO_EIGHTH_BIT = 0x7FL << 56;

	MemorySegment[] getSegments();

	int getOffset();

	int getSizeInBytes();

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
			MemorySegment[] segments,
			int baseOffset,
			int fieldOffset,
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
			MemorySegment[] segments,
			int baseOffset,
			int fieldOffset,
			long variablePartOffsetAndLen) {
		long mark = variablePartOffsetAndLen & HIGHEST_FIRST_BIT;
		if (mark == 0) {
			final int subOffset = (int) (variablePartOffsetAndLen >> 32);
			final int len = (int) variablePartOffsetAndLen;
			return BinaryString.fromAddress(segments, baseOffset + subOffset, len);
		} else {
			int len = (int) ((variablePartOffsetAndLen & HIGHEST_SECOND_TO_EIGHTH_BIT) >>> 56);
			if (SegmentsUtil.LITTLE_ENDIAN) {
				return BinaryString.fromAddress(segments, fieldOffset, len);
			} else {
				// fieldOffset + 1 to skip header.
				return BinaryString.fromAddress(segments, fieldOffset + 1, len);
			}
		}
	}
}
