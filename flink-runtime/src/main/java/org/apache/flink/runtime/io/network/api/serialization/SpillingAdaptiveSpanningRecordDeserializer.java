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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * @param <T> The type of the record to be deserialized.
 */
public class SpillingAdaptiveSpanningRecordDeserializer<T extends IOReadableWritable> implements RecordDeserializer<T> {

	private static final String BROKEN_SERIALIZATION_ERROR_MESSAGE =
		"Serializer consumed more bytes than the record had. " +
			"This indicates broken serialization. If you are using custom serialization types " +
			"(Value or Writable), check their serialization methods. If you are using a " +
			"Kryo-serialized type, check the corresponding Kryo serializer.";

	private final NonSpanningWrapper nonSpanningWrapper;

	private final SpanningWrapper spanningWrapper;

	private Buffer currentBuffer;

	public SpillingAdaptiveSpanningRecordDeserializer(String[] tmpDirectories) {
		this.nonSpanningWrapper = new NonSpanningWrapper();
		this.spanningWrapper = new SpanningWrapper(tmpDirectories);
	}

	@Override
	public void setNextBuffer(Buffer buffer) throws IOException {
		currentBuffer = buffer;

		int offset = buffer.getMemorySegmentOffset();
		MemorySegment segment = buffer.getMemorySegment();
		int numBytes = buffer.getSize();

		// check if some spanning record deserialization is pending
		if (this.spanningWrapper.getNumGatheredBytes() > 0) {
			this.spanningWrapper.addNextChunkFromMemorySegment(segment, offset, numBytes);
		}
		else {
			this.nonSpanningWrapper.initializeFromMemorySegment(segment, offset, numBytes + offset);
		}
	}

	@Override
	public Buffer getCurrentBuffer () {
		Buffer tmp = currentBuffer;
		currentBuffer = null;
		return tmp;
	}

	@Override
	public DeserializationResult getNextRecord(T target) throws IOException {
		// always check the non-spanning wrapper first.
		// this should be the majority of the cases for small records
		// for large records, this portion of the work is very small in comparison anyways

		int nonSpanningRemaining = this.nonSpanningWrapper.remaining();

		// check if we can get a full length;
		if (nonSpanningRemaining >= 4) {
			int len = this.nonSpanningWrapper.readInt();

			if (len <= nonSpanningRemaining - 4) {
				// we can get a full record from here
				try {
					target.read(this.nonSpanningWrapper);

					int remaining = this.nonSpanningWrapper.remaining();
					if (remaining > 0) {
						return DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
					}
					else if (remaining == 0) {
						return DeserializationResult.LAST_RECORD_FROM_BUFFER;
					}
					else {
						throw new IndexOutOfBoundsException("Remaining = " + remaining);
					}
				}
				catch (IndexOutOfBoundsException e) {
					throw new IOException(BROKEN_SERIALIZATION_ERROR_MESSAGE, e);
				}
			}
			else {
				// we got the length, but we need the rest from the spanning deserializer
				// and need to wait for more buffers
				this.spanningWrapper.initializeWithPartialRecord(this.nonSpanningWrapper, len);
				this.nonSpanningWrapper.clear();
				return DeserializationResult.PARTIAL_RECORD;
			}
		} else if (nonSpanningRemaining > 0) {
			// we have an incomplete length
			// add our part of the length to the length buffer
			this.spanningWrapper.initializeWithPartialLength(this.nonSpanningWrapper);
			this.nonSpanningWrapper.clear();
			return DeserializationResult.PARTIAL_RECORD;
		}

		// spanning record case
		if (this.spanningWrapper.hasFullRecord()) {
			// get the full record
			target.read(this.spanningWrapper.getInputView());

			// move the remainder to the non-spanning wrapper
			// this does not copy it, only sets the memory segment
			this.spanningWrapper.moveRemainderToNonSpanningDeserializer(this.nonSpanningWrapper);
			this.spanningWrapper.clear();

			return (this.nonSpanningWrapper.remaining() == 0) ?
				DeserializationResult.LAST_RECORD_FROM_BUFFER :
				DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
		} else {
			return DeserializationResult.PARTIAL_RECORD;
		}
	}

	@Override
	public void clear() {
		this.nonSpanningWrapper.clear();
		this.spanningWrapper.clear();
	}

	@Override
	public boolean hasUnfinishedData() {
		return this.nonSpanningWrapper.remaining() > 0 || this.spanningWrapper.getNumGatheredBytes() > 0;
	}

}
