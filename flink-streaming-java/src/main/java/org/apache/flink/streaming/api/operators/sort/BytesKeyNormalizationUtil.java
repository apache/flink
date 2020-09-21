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

package org.apache.flink.streaming.api.operators.sort;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.streaming.api.operators.sort.FixedLengthByteKeyComparator.TIMESTAMP_BYTE_SIZE;

/**
 * Utility class for common key normalization used both in {@link VariableLengthByteKeyComparator}
 * and {@link FixedLengthByteKeyComparator}.
 */
final class BytesKeyNormalizationUtil {
	/**
	 * Writes the normalized key of given record. The normalized key consists of the key serialized as bytes and
	 * the timestamp of the record.
	 *
	 * <p>NOTE: The key does not represent a logical order. It can be used only for grouping keys!
	 */
	static <IN> void putNormalizedKey(
			Tuple2<byte[], StreamRecord<IN>> record,
			int dataLength,
			MemorySegment target,
			int offset,
			int numBytes) {
		byte[] data = record.f0;

		if (dataLength >= numBytes) {
			putBytesArray(target, offset, numBytes, data);
		} else {
			// whole key fits into the normalized key
			putBytesArray(target, offset, dataLength, data);
			int lastOffset = offset + numBytes;
			offset += dataLength;
			long valueOfTimestamp = record.f1.asRecord().getTimestamp() - Long.MIN_VALUE;
			if (dataLength + TIMESTAMP_BYTE_SIZE <= numBytes) {
				// whole timestamp fits into the normalized key
				target.putLong(offset, valueOfTimestamp);
				offset += TIMESTAMP_BYTE_SIZE;
				// fill in the remaining space with zeros
				while (offset < lastOffset) {
					target.put(offset++, (byte) 0);
				}
			} else {
				// only part of the timestamp fits into normalized key
				for (int i = 0; offset < lastOffset; offset++, i++) {
					target.put(offset, (byte) (valueOfTimestamp >>> ((7 - i) << 3)));
				}
			}
		}
	}

	private static void putBytesArray(MemorySegment target, int offset, int numBytes, byte[] data) {
		for (int i = 0; i < numBytes; i++) {
			// We're converting the signed byte in data into an unsigned representation.
			// A Java byte goes from -127 to 128, i.e. is signed. By subtracting -127 (MIN_VALUE)
			// here we're shifting the number to be from 0 to 255. The normalized key sorter sorts
			// bytes as "unsigned", so we need to convert here to maintain a correct ordering.
			int highByte = data[i] & 0xff;
			highByte -= Byte.MIN_VALUE;
			target.put(offset + i, (byte) highByte);
		}
	}

	private BytesKeyNormalizationUtil() {
	}
}
