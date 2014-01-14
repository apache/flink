/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.serialization;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.runtime.io.Buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SpanningRecordSerializer<T extends IOReadableWritable> implements RecordSerializer<T> {

	/** Flag to enable/disable checks, if buffer not set/full or pending serialization */
	private static final boolean CHECKED = true;

	/** Intermediate data serialization */
	private final DataOutputSerializer serializationBuffer;

	/** Intermediate buffer for data serialization */
	private ByteBuffer dataBuffer;

	/** Intermediate buffer for length serialization */
	private final ByteBuffer lengthBuffer;

	/** Current target {@link eu.stratosphere.runtime.io.Buffer} of the serializer */
	private Buffer targetBuffer;

	/** Position in current {@link MemorySegment} of target buffer */
	private int position;

	/** Limit of current {@link MemorySegment} of target buffer */
	private int limit;

	public SpanningRecordSerializer() {
		this.serializationBuffer = new DataOutputSerializer(128);

		this.lengthBuffer = ByteBuffer.allocate(4);
		this.lengthBuffer.order(ByteOrder.BIG_ENDIAN);

		// ensure initial state with hasRemaining false (for correct setNextBuffer logic)
		this.dataBuffer = this.serializationBuffer.wrapAsByteBuffer();
		this.lengthBuffer.position(4);
	}

	@Override
	public SerializationResult addRecord(T record) throws IOException {
		if (CHECKED) {
			if (this.dataBuffer.hasRemaining()) {
				throw new IllegalStateException("Pending serialization of previous record.");
			}
		}

		this.serializationBuffer.clear();
		this.lengthBuffer.clear();

		// write data and length
		record.write(this.serializationBuffer);
		this.lengthBuffer.putInt(0, this.serializationBuffer.length());

		this.dataBuffer = this.serializationBuffer.wrapAsByteBuffer();

		// Copy from intermediate buffers to current target memory segment
		copyToTargetBufferFrom(this.lengthBuffer);
		copyToTargetBufferFrom(this.dataBuffer);

		return getSerializationResult();
	}

	@Override
	public SerializationResult setNextBuffer(Buffer buffer) throws IOException {
		this.targetBuffer = buffer;
		this.position = 0;
		this.limit = buffer.size();

		if (this.lengthBuffer.hasRemaining()) {
			copyToTargetBufferFrom(this.lengthBuffer);
		}

		if (this.dataBuffer.hasRemaining()) {
			copyToTargetBufferFrom(this.dataBuffer);
		}

		return getSerializationResult();
	}

	/**
	 * Copies as many bytes as possible from the given {@link ByteBuffer} to the {@link MemorySegment} of the target
	 * {@link Buffer} and advances the current position by the number of written bytes.
	 *
	 * @param source the {@link ByteBuffer} to copy data from
	 */
	private void copyToTargetBufferFrom(ByteBuffer source) {
		if (this.targetBuffer == null)
			return;

		int needed = source.remaining();
		int available = this.limit - this.position;
		int toCopy = Math.min(needed, available);

		this.targetBuffer.getMemorySegment().put(this.position, source, toCopy);

		this.position += toCopy;
	}

	private SerializationResult getSerializationResult() {
		if (!this.dataBuffer.hasRemaining() && !this.lengthBuffer.hasRemaining()) {
			return (this.position < this.limit)
					? SerializationResult.FULL_RECORD
					: SerializationResult.FULL_RECORD_MEMORY_SEGMENT_FULL;
		}

		return SerializationResult.PARTIAL_RECORD_MEMORY_SEGMENT_FULL;
	}

	@Override
	public Buffer getCurrentBuffer() {
		if (targetBuffer == null)
			return null;

		this.targetBuffer.limitSize(this.position);
		return this.targetBuffer;
	}

	@Override
	public void clear() {
		this.targetBuffer = null;
		this.position = 0;
		this.limit = 0;

		// ensure clear state with hasRemaining false (for correct setNextBuffer logic)
		this.dataBuffer.position(this.dataBuffer.limit());
		this.lengthBuffer.position(4);
	}

	@Override
	public boolean hasData() {
		// either data in current target buffer or intermediate buffers
		return this.position > 0 || (this.lengthBuffer.hasRemaining() || this.dataBuffer.hasRemaining());
	}
}
