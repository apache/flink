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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Record serializer which serializes the complete record to an intermediate
 * data serialization buffer and copies this buffer to target buffers
 * one-by-one using {@link #copyToBufferBuilder(BufferBuilder)}.
 *
 * @param <T> The type of the records that are serialized.
 */
public class CompositeSpanningRecordSerializer<T extends IOReadableWritable> implements RecordSerializer<T> {

	/** Intermediate buffer to hold serialized data. */
	private final DataOutputSerializer serializationBuffer;

	/** The max data size to hold in {@code serializationBuffer}. */
	private final int maxBufferLenForInternalSer;

	/** The {@link RecordSerializer} to transform intermediate buffers into other buffers. */
	private final RecordSerializer<BufferSerializationDelegate> subSerializer;

	/** The implementation of {@link BufferSerializationDelegate} to transform a single buffer into other buffers. */
	private final BufferSerializationDelegate subSerializationDelegate;

	/** {@link SerializationResult} of {@code subSerializer} used to decide whether there is data ready to be fetched. */
	private SerializationResult subSerializationResult;

	/** A temporary wrapper of data in {@code serializationBuffer} to feed {@code subSerializer}. */
	private ByteBuffer subSerializationBuffer;

	public CompositeSpanningRecordSerializer(RecordSerializer<BufferSerializationDelegate> subSerializer,
		BufferSerializationDelegate subSerializationDelegate, int subSerBufferLength) {

		checkNotNull(subSerializer);
		checkNotNull(subSerializationDelegate);

		this.subSerializer = subSerializer;
		this.subSerializationDelegate = subSerializationDelegate;
		this.maxBufferLenForInternalSer = subSerBufferLength;

		this.serializationBuffer = new DataOutputSerializer(subSerBufferLength << 1);
		this.subSerializationResult = SerializationResult.FULL_RECORD;
	}

	/**
	 * Serializes the complete record to an intermediate data serialization buffer.
	 *
	 * @param record the record to serialize
	 */
	@Override
	public void serializeRecord(T record) throws IOException {
		// Reserve 4 bytes for the length.
		int prevPosition = serializationBuffer.position();
		serializationBuffer.skipBytesToWrite(4);

		record.write(serializationBuffer);

		// Write length field.
		int newPosition = serializationBuffer.position();
		serializationBuffer.position(prevPosition);
		serializationBuffer.writeInt(newPosition - prevPosition - 4);
		serializationBuffer.position(newPosition);
	}

	private SerializationResult copyOrFlushToBufferBuilder(BufferBuilder targetBuffer, boolean needFlush) {
		int currentInternalBufferLimit = !needFlush ? maxBufferLenForInternalSer : 1;
		if (subSerializationResult == SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL) {
			subSerializationResult = !needFlush
				? subSerializer.copyToBufferBuilder(targetBuffer)
				: subSerializer.flushToBufferBuilder(targetBuffer);
			if (subSerializationResult == SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL) {
				if (subSerializationBuffer == null && serializationBuffer.length() < currentInternalBufferLimit) {
					return SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL;
				} else {
					// Data in sub-serializer has been fully consumed while the data in serializationBuffer
					// may be able to feed sub-serializer.
					return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
				}
			} else if (subSerializationResult == SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL){
				return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
			}
		}
		// Data in sub-serializer has been fully consumed while there is room in targetBuffer.
		if (subSerializationBuffer == null) {
			if (serializationBuffer.length() < currentInternalBufferLimit) {
				return SerializationResult.FULL_RECORD;
			} else {
				subSerializationBuffer = serializationBuffer.wrapAsByteBuffer();
			}
		}
		while (subSerializationBuffer != null) {
			int position = subSerializationBuffer.position();
			int nextLen = Math.min(subSerializationBuffer.remaining(), maxBufferLenForInternalSer);
			ByteBuffer nextBufferForInternalSer = ByteBuffer.wrap(subSerializationBuffer.array(), position, nextLen);
			subSerializationDelegate.setBuffer(nextBufferForInternalSer);
			try {
				subSerializer.serializeRecord(subSerializationDelegate);
			} catch (IOException e) {
				// Actually there won't be any IOException since internal storage is backed by memory.
				throw new RuntimeException(e);
			}

			subSerializer.reset();
			subSerializationResult = subSerializer.copyToBufferBuilder(targetBuffer);

			subSerializationBuffer.position(position + nextLen);
			if (subSerializationBuffer.remaining() < currentInternalBufferLimit) {
				int nextPosition = subSerializationBuffer.remaining();
				if (subSerializationBuffer.remaining() > 0) {
					// The last segment is not enough for a full buffer, copy the remaining data to the front.
					System.arraycopy(serializationBuffer.getSharedBuffer(), subSerializationBuffer.position(),
						serializationBuffer.getSharedBuffer(), 0,
						subSerializationBuffer.remaining());
				}
				serializationBuffer.clear();
				serializationBuffer.position(nextPosition);
				subSerializationBuffer = null;
				break;
			}
			if (subSerializationResult.isFullBuffer()) {
				break;
			}
		}
		if (subSerializationResult == SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL || subSerializationBuffer != null) {
			return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
		} else {
			return !targetBuffer.isFull()
				? SerializationResult.FULL_RECORD
				: SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL;
		}
	}

	/**
	 * Copies the intermediate data serialization buffer to target BufferBuilder.
	 *
	 * @param targetBuffer the target BufferBuilder to copy to
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 */
	@Override
	public SerializationResult copyToBufferBuilder(BufferBuilder targetBuffer) {
		return copyOrFlushToBufferBuilder(targetBuffer, false);
	}

	@Override
	public SerializationResult flushToBufferBuilder(BufferBuilder targetBuffer) {
		return copyOrFlushToBufferBuilder(targetBuffer, true);
	}

	@Override
	public void reset() {
		subSerializer.reset();
	}

	@Override
	public void prune() {
		serializationBuffer.pruneBuffer();
		subSerializer.prune();
	}

	@Override
	public boolean hasSerializedData() {
		return serializationBuffer.length() > 0 || subSerializationBuffer != null || subSerializer.hasSerializedData();
	}
}
