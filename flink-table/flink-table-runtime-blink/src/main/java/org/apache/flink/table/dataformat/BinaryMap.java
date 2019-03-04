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
import org.apache.flink.table.util.SegmentsUtil;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * [4 byte(keyArray size in bytes)] + [Key BinaryArray] + [Value BinaryArray].
 *
 * <p>{@code BinaryMap} are influenced by Apache Spark UnsafeMapData.
 */
public class BinaryMap extends BinaryFormat {

	private final BinaryArray keys;
	private final BinaryArray values;

	public BinaryMap() {
		keys = new BinaryArray();
		values = new BinaryArray();
	}

	public int numElements() {
		return keys.numElements();
	}

	public void pointTo(MemorySegment segment, int baseOffset, int sizeInBytes) {
		pointTo(new MemorySegment[]{segment}, baseOffset, sizeInBytes);
	}

	public void pointTo(MemorySegment[] segments, int offset, int sizeInBytes) {
		// Read the numBytes of key array from the first 4 bytes.
		final int keyArrayBytes = SegmentsUtil.getInt(segments, offset);
		assert keyArrayBytes >= 0 : "keyArraySize (" + keyArrayBytes + ") should >= 0";
		final int valueArrayBytes = sizeInBytes - keyArrayBytes - 4;
		assert valueArrayBytes >= 0 : "valueArraySize (" + valueArrayBytes + ") should >= 0";

		keys.pointTo(segments, offset + 4, keyArrayBytes);
		values.pointTo(segments, offset + 4 + keyArrayBytes, valueArrayBytes);

		assert keys.numElements() == values.numElements();

		this.segments = segments;
		this.offset = offset;
		this.sizeInBytes = sizeInBytes;
	}

	public BinaryArray keyArray() {
		return keys;
	}

	public BinaryArray valueArray() {
		return values;
	}

	public BinaryMap copy() {
		return copy(new BinaryMap());
	}

	public BinaryMap copy(BinaryMap reuse) {
		byte[] bytes = SegmentsUtil.copyToBytes(segments, offset, sizeInBytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, sizeInBytes);
		return reuse;
	}

	public static BinaryMap valueOf(BinaryArray key, BinaryArray value) {
		checkArgument(key.getSegments().length == 1 && value.getSegments().length == 1);
		byte[] bytes = new byte[4 + key.getSizeInBytes() + value.getSizeInBytes()];
		MemorySegment segment = MemorySegmentFactory.wrap(bytes);
		segment.putInt(0, key.getSizeInBytes());
		key.getSegments()[0].copyTo(key.getOffset(), segment, 4, key.getSizeInBytes());
		value.getSegments()[0].copyTo(
				value.getOffset(), segment, 4 + key.getSizeInBytes(), value.getSizeInBytes());
		BinaryMap map = new BinaryMap();
		map.pointTo(segment, 0, bytes.length);
		return map;
	}

	public static BinaryMap readBinaryMapFieldFromSegments(
			MemorySegment[] segments, int baseOffset, long offsetAndSize) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		BinaryMap map = new BinaryMap();
		map.pointTo(segments, offset + baseOffset, size);
		return map;
	}
}
