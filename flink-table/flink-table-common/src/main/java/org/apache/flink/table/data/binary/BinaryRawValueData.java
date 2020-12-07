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

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * A lazily binary implementation of {@link RawValueData} which is backed by {@link MemorySegment}s
 * and generic {@link Object}.
 *
 * <p>Either {@link MemorySegment}s or {@link Object} must be provided when
 * constructing {@link BinaryRawValueData}. The other representation will be materialized when needed.
 *
 * @param <T> the java type of the raw value.
 */
@Internal
public final class BinaryRawValueData<T> extends LazyBinaryFormat<T> implements RawValueData<T> {

	public BinaryRawValueData(T javaObject) {
		super(javaObject);
	}

	public BinaryRawValueData(MemorySegment[] segments, int offset, int sizeInBytes) {
		super(segments, offset, sizeInBytes);
	}

	public BinaryRawValueData(MemorySegment[] segments, int offset, int sizeInBytes, T javaObject) {
		super(segments, offset, sizeInBytes, javaObject);
	}

	// ------------------------------------------------------------------------------------------
	// Public Interfaces
	// ------------------------------------------------------------------------------------------

	@Override
	public T toObject(TypeSerializer<T> serializer) {
		if (javaObject == null) {
			try {
				javaObject = InstantiationUtil.deserializeFromByteArray(
					serializer,
					toBytes(serializer));
			} catch (IOException e) {
				throw new FlinkRuntimeException(e);
			}
		}
		return javaObject;
	}

	@Override
	public byte[] toBytes(TypeSerializer<T> serializer) {
		ensureMaterialized(serializer);
		return BinarySegmentUtils.copyToBytes(getSegments(), getOffset(), getSizeInBytes());
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BinaryRawValueData) {
			BinaryRawValueData<?> other = (BinaryRawValueData<?>) o;
			if (binarySection != null && other.binarySection != null) {
				return binarySection.equals(other.binarySection);
			}
			throw new UnsupportedOperationException(
				"Unmaterialized BinaryRawValueData cannot be compared.");
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		if (binarySection != null) {
			return binarySection.hashCode();
		}
		throw new UnsupportedOperationException(
			"Unmaterialized BinaryRawValueData does not have a hashCode.");
	}

	@Override
	public String toString() {
		return String.format("SqlRawValue{%s}", javaObject == null ? "?" : javaObject);
	}

	// ------------------------------------------------------------------------------------
	// Internal methods
	// ------------------------------------------------------------------------------------

	@Override
	protected BinarySection materialize(TypeSerializer<T> serializer) {
		try {
			byte[] bytes = InstantiationUtil.serializeToByteArray(serializer, javaObject);
			return new BinarySection(new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, 0, bytes.length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// ------------------------------------------------------------------------------------------
	// Construction Utilities
	// ------------------------------------------------------------------------------------------

	/**
	 * Creates a {@link BinaryRawValueData} instance from the given Java object.
	 */
	public static <T> BinaryRawValueData<T> fromObject(T javaObject) {
		if (javaObject == null) {
			return null;
		}
		return new BinaryRawValueData<>(javaObject);
	}

	/**
	 * Creates a {@link BinaryStringData} instance from the given bytes.
	 */
	public static <T> BinaryRawValueData<T> fromBytes(byte[] bytes) {
		return fromBytes(bytes, 0, bytes.length);
	}

	/**
	 * Creates a {@link BinaryStringData} instance from the given bytes with offset and number of bytes.
	 */
	public static <T> BinaryRawValueData<T> fromBytes(byte[] bytes, int offset, int numBytes) {
		return new BinaryRawValueData<>(
			new MemorySegment[] {MemorySegmentFactory.wrap(bytes)},
			offset,
			numBytes);
	}
}
