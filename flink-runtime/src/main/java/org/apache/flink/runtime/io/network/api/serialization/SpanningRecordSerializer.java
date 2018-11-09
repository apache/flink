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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Record serializer which serializes the complete record to an intermediate
 * data serialization buffer and copies this buffer to target buffers
 * one-by-one using {@link #copyToBufferBuilder(BufferBuilder)}.
 *
 * @param <T> The type of the records that are serialized.
 */
public class SpanningRecordSerializer<T extends IOReadableWritable> implements RecordSerializer<T> {

	/** Flag to enable/disable checks, if buffer not set/full or pending serialization. */
	private static final boolean CHECKED = false;

	/** Intermediate data serialization. */
	private final DataOutputSerializer serializationBuffer;

	/** Intermediate buffer for data serialization (wrapped from {@link #serializationBuffer}). */
	private ByteBuffer dataBuffer;

	/** Intermediate buffer for length serialization. */
	private final ByteBuffer lengthBuffer;

	public SpanningRecordSerializer() {
		serializationBuffer = new DataOutputSerializer(128);

		lengthBuffer = ByteBuffer.allocate(4);
		lengthBuffer.order(ByteOrder.BIG_ENDIAN);

		// ensure initial state with hasRemaining false (for correct continueWritingWithNextBufferBuilder logic)
		dataBuffer = serializationBuffer.wrapAsByteBuffer();
		lengthBuffer.position(4);
	}

	/**
	 * Serializes the complete record to an intermediate data serialization buffer.
	 *
	 * @param record the record to serialize
	 */
	@Override
	public void serializeRecord(T record) throws IOException {
		if (CHECKED) {
			if (dataBuffer.hasRemaining()) {
				throw new IllegalStateException("Pending serialization of previous record.");
			}
		}

		serializationBuffer.clear();
		lengthBuffer.clear();

		// write data and length
		record.write(serializationBuffer);

		int len = serializationBuffer.length();
		lengthBuffer.putInt(0, len);

		dataBuffer = serializationBuffer.wrapAsByteBuffer();
	}

	/**
	 * Copies an intermediate data serialization buffer into the target BufferBuilder.
	 *
	 * @param targetBuffer the target BufferBuilder to copy to
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 */
	@Override
	public SerializationResult copyToBufferBuilder(BufferBuilder targetBuffer) {
		targetBuffer.append(lengthBuffer);
		targetBuffer.append(dataBuffer);
		targetBuffer.commit();

		return getSerializationResult(targetBuffer);
	}

	private SerializationResult getSerializationResult(BufferBuilder targetBuffer) {
		if (dataBuffer.hasRemaining() || lengthBuffer.hasRemaining()) {
			return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
		}
		return !targetBuffer.isFull()
			? SerializationResult.FULL_RECORD
			: SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL;
	}

	@Override
	public void reset() {
		dataBuffer.position(0);
		lengthBuffer.position(0);
	}

	@Override
	public void prune() {
		serializationBuffer.pruneBuffer();
		dataBuffer = serializationBuffer.wrapAsByteBuffer();
	}

	@Override
	public boolean hasSerializedData() {
		return lengthBuffer.hasRemaining() || dataBuffer.hasRemaining();
	}
}
