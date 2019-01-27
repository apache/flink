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
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.IOException;

/**
 * @param <T> The type of the record to be deserialized.
 */
public class CompositeSpillingAdaptiveSpanningRecordDeserializer<T extends IOReadableWritable> implements RecordDeserializer<T> {

	/** Inner deserializer which deserializes buffers to targets of type T. */
	private final SpillingAdaptiveSpanningRecordDeserializer<T> innerDeserializer;

	/** The previous deserialization result of inner deserializer. */
	private DeserializationResult innerDeserializationResult;

	/** Sub-deserializer transform original buffers set to this {@link RecordSerializer} through
	 *  {@link RecordDeserializer#setNextBuffer(Buffer)} into other buffers which will be consumed by inner deserializer. */
	private final RecordDeserializer<BufferDeserializationDelegate> subDeserializer;

	/** The target to deserialize buffers from sub-deserializer. */
	private final BufferDeserializationDelegate subDeserializationDelegate;

	/** The previous deserialization result of sub-deserializer. */
	private DeserializationResult subDeserializationResult;

	public CompositeSpillingAdaptiveSpanningRecordDeserializer(String[] tmpDirectories,
		RecordDeserializer<BufferDeserializationDelegate> subDeserializer,
		BufferDeserializationDelegate subDeserializationDelegate) {

		assert subDeserializer != null;
		assert subDeserializationDelegate != null;

		this.innerDeserializer = new SpillingAdaptiveSpanningRecordDeserializer<>(tmpDirectories);
		this.innerDeserializationResult = DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
		this.subDeserializer = subDeserializer;
		this.subDeserializationDelegate = subDeserializationDelegate;
		this.subDeserializationResult = DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
	}

	@Override
	public void setNextBuffer(Buffer buffer) throws IOException {
		subDeserializer.setNextBuffer(buffer);

		// The deserialization result of sub-deserializer will change once setNextBuffer(),
		// while the deserialization result of inner deserializer will only change during getNextRecord().
		subDeserializationResult = DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
	}

	@Override
	public Buffer getCurrentBuffer () {
		return subDeserializer.getCurrentBuffer();
	}

	@Override
	public DeserializationResult getNextRecord(T target) throws IOException {
		// No need to deserialize another buffer from sub-deserializer if inner deserializer hasn't fully consumed its buffer.
		if (innerDeserializationResult == DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER) {
			innerDeserializationResult = innerDeserializer.getNextRecord(target);
		} else if (innerDeserializationResult == DeserializationResult.LAST_RECORD_FROM_BUFFER) {
			// The result of inner deserialization should be PARTIAL_RECORD this time since there is not more data available.
			innerDeserializationResult = DeserializationResult.PARTIAL_RECORD;
		}

		// If a new record is deserialized from inner deserializer, just return.
		if (innerDeserializationResult.isFullRecord()) {
			// If the buffer in inner deserializer is fully consumed, try to add one more buffer from sub-deserializer.
			if (innerDeserializationResult == DeserializationResult.LAST_RECORD_FROM_BUFFER
				&& subDeserializationResult == DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER) {
				subDeserializationDelegate.reset();
				subDeserializationResult = subDeserializer.getNextRecord(subDeserializationDelegate);
				if (subDeserializationResult.isFullRecord()) {
					innerDeserializer.setNextBuffer(subDeserializationDelegate.getBuffer());
					innerDeserializationResult = DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
				}
			}
			// Reset inner deserialization result considering previous sub-deserialization result.
			if (innerDeserializationResult == DeserializationResult.LAST_RECORD_FROM_BUFFER &&
				subDeserializationResult == DeserializationResult.LAST_RECORD_FROM_BUFFER) {
				return DeserializationResult.LAST_RECORD_FROM_BUFFER;
			} else {
				return DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
			}
		}

		// Try to consume more buffers from sub-deserializer to feed inner deserializer
		// if innerDeserializationResult is PARTIAL_RECORD.
		while (innerDeserializationResult != DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER
			&& subDeserializationResult == DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER) {

			subDeserializationDelegate.reset();
			subDeserializationResult = subDeserializer.getNextRecord(subDeserializationDelegate);
			if (subDeserializationResult.isFullRecord()) {
				innerDeserializer.setNextBuffer(subDeserializationDelegate.getBuffer());
				if (!innerDeserializationResult.isFullRecord()) {
					innerDeserializationResult = innerDeserializer.getNextRecord(target);
				} else {
					innerDeserializationResult = DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
					break;
				}
			} else {
				break;
			}
		}

		if (innerDeserializationResult.isFullRecord()) {
			if (!innerDeserializationResult.isBufferConsumed() || subDeserializationResult != DeserializationResult.LAST_RECORD_FROM_BUFFER) {
				return DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
			} else {
				return DeserializationResult.LAST_RECORD_FROM_BUFFER;
			}
		} else {
			return DeserializationResult.PARTIAL_RECORD;
		}
	}

	@Override
	public void clear() {
		this.innerDeserializer.clear();
		this.innerDeserializationResult = DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
		this.subDeserializer.clear();
		this.subDeserializationDelegate.clear();
		this.subDeserializationResult = DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
	}

	@Override
	public boolean hasUnfinishedData() {
		return innerDeserializer.hasUnfinishedData() || subDeserializer.hasUnfinishedData();
	}

}
