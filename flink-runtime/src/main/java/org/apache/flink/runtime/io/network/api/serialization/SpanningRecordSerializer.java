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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Record serializer which serializes the complete record to an intermediate
 * data serialization buffer and copies this buffer to target buffers
 * one-by-one using {@link #setNextBufferBuilder(BufferBuilder)}.
 *
 * @param <T>
 */
public class SpanningRecordSerializer<T extends IOReadableWritable> implements RecordSerializer<T> {

	/** Flag to enable/disable checks, if buffer not set/full or pending serialization */
	private static final boolean CHECKED = false;

	/** Intermediate data serialization */
	private final DataOutputSerializer serializationBuffer;

	/** Intermediate buffer for data serialization (wrapped from {@link #serializationBuffer}) */
	private ByteBuffer dataBuffer;

	/** Intermediate buffer for length serialization */
	private final ByteBuffer lengthBuffer;

	/** Current target {@link Buffer} of the serializer */
	@Nullable
	private BufferBuilder targetBuffer;

	public SpanningRecordSerializer() {
		serializationBuffer = new DataOutputSerializer(128);

		lengthBuffer = ByteBuffer.allocate(4);
		lengthBuffer.order(ByteOrder.BIG_ENDIAN);

		// ensure initial state with hasRemaining false (for correct setNextBufferBuilder logic)
		dataBuffer = serializationBuffer.wrapAsByteBuffer();
		lengthBuffer.position(4);
	}

	/**
	 * Serializes the complete record to an intermediate data serialization
	 * buffer and starts copying it to the target buffer (if available).
	 *
	 * @param record the record to serialize
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 * @throws IOException
	 */
	@Override
	public SerializationResult addRecord(T record) throws IOException {
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

		// Copy from intermediate buffers to current target memory segment
		copyToTargetBufferFrom(lengthBuffer);
		copyToTargetBufferFrom(dataBuffer);

		return getSerializationResult();
	}

	@Override
	public SerializationResult setNextBufferBuilder(BufferBuilder buffer) throws IOException {
		targetBuffer = buffer;

		if (lengthBuffer.hasRemaining()) {
			copyToTargetBufferFrom(lengthBuffer);
		}

		if (dataBuffer.hasRemaining()) {
			copyToTargetBufferFrom(dataBuffer);
		}

		SerializationResult result = getSerializationResult();
		
		// make sure we don't hold onto the large buffers for too long
		if (result.isFullRecord()) {
			serializationBuffer.clear();
			serializationBuffer.pruneBuffer();
			dataBuffer = serializationBuffer.wrapAsByteBuffer();
		}
		
		return result;
	}

	/**
	 * Copies as many bytes as possible from the given {@link ByteBuffer} to the {@link MemorySegment} of the target
	 * {@link Buffer} and advances the current position by the number of written bytes.
	 *
	 * @param source the {@link ByteBuffer} to copy data from
	 */
	private void copyToTargetBufferFrom(ByteBuffer source) {
		if (targetBuffer == null) {
			return;
		}
		targetBuffer.append(source);
	}

	private SerializationResult getSerializationResult() {
		if (!dataBuffer.hasRemaining() && !lengthBuffer.hasRemaining()) {
			return !targetBuffer.isFull()
					? SerializationResult.FULL_RECORD
					: SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL;
		}

		return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
	}

	@Override
	public Buffer getCurrentBuffer() {
		if (targetBuffer == null) {
			return null;
		}
		Buffer result = targetBuffer.build();
		targetBuffer = null;
		return result;
	}

	@Override
	public void clearCurrentBuffer() {
		targetBuffer = null;
	}

	@Override
	public void clear() {
		targetBuffer = null;

		// ensure clear state with hasRemaining false (for correct setNextBufferBuilder logic)
		dataBuffer.position(dataBuffer.limit());
		lengthBuffer.position(4);
	}

	@Override
	public boolean hasData() {
		// either data in current target buffer or intermediate buffers
		return (targetBuffer != null && !targetBuffer.isEmpty()) || lengthBuffer.hasRemaining() || dataBuffer.hasRemaining();
	}
}
