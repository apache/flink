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
import org.apache.flink.table.type.GenericType;
import org.apache.flink.table.util.SegmentsUtil;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * Internal format to {@link GenericType}.
 */
public class BinaryGeneric<T> extends LazyBinaryFormat<T> {

	public BinaryGeneric() {}

	public BinaryGeneric(T javaObject) {
		super(null, -1, -1, javaObject);
	}

	public BinaryGeneric(MemorySegment[] segments, int offset, int sizeInBytes) {
		super(segments, offset, sizeInBytes, null);
	}

	public BinaryGeneric(MemorySegment[] segments, int offset, int sizeInBytes, T javaObject) {
		super(segments, offset, sizeInBytes, javaObject);
	}

	public void ensureMaterialized(TypeSerializer<T> serializer) {
		if (segments == null) {
			materialize(serializer);
		}
	}

	public void ensureJavaObject(TypeSerializer<T> serializer) {
		if (javaObject == null) {
			makeJavaObject(serializer);
		}
	}

	public void materialize(TypeSerializer<T> serializer) {
		try {
			byte[] bytes = InstantiationUtil.serializeToByteArray(serializer, javaObject);
			pointTo(new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, 0, bytes.length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void makeJavaObject(TypeSerializer<T> serializer) {
		try {
			javaObject = InstantiationUtil.deserializeFromByteArray(
					serializer, SegmentsUtil.copyToBytes(segments, offset, sizeInBytes));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public BinaryGeneric<T> copy() {
		byte[] bytes = SegmentsUtil.copyToBytes(segments, offset, sizeInBytes);
		return new BinaryGeneric<>(new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, 0, sizeInBytes);
	}

	static BinaryGeneric readBinaryGenericFieldFromSegments(
			MemorySegment[] segments, int baseOffset, long offsetAndSize) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		return new BinaryGeneric(segments, offset + baseOffset, size);
	}
}
