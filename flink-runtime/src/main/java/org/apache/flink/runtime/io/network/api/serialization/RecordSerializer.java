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
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;

/**
 * Interface for turning records into sequences of memory segments.
 */
public interface RecordSerializer<T extends IOReadableWritable> {

	/**
	 * Status of the serialization result.
	 */
	enum SerializationResult {
		PARTIAL_RECORD_MEMORY_SEGMENT_FULL(false, true),
		FULL_RECORD_MEMORY_SEGMENT_FULL(true, true),
		FULL_RECORD(true, false);

		private final boolean isFullRecord;

		private final boolean isFullBuffer;

		SerializationResult(boolean isFullRecord, boolean isFullBuffer) {
			this.isFullRecord = isFullRecord;
			this.isFullBuffer = isFullBuffer;
		}

		/**
		 * Whether the full record was serialized and completely written to
		 * a target buffer.
		 *
		 * @return <tt>true</tt> if the complete record was written
		 */
		public boolean isFullRecord() {
			return this.isFullRecord;
		}

		/**
		 * Whether the target buffer is full after the serialization process.
		 *
		 * @return <tt>true</tt> if the target buffer is full
		 */
		public boolean isFullBuffer() {
			return this.isFullBuffer;
		}
	}

	/**
	 * Starts serializing the given record to an intermediate data buffer.
	 *
	 * @param record the record to serialize
	 */
	void serializeRecord(T record) throws IOException;

	/**
	 * Copies the intermediate data serialization buffer to the given target buffer.
	 *
	 * @param bufferBuilder the new target buffer to use
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 */
	SerializationResult copyToBufferBuilder(BufferBuilder bufferBuilder);

	/**
	 * Clears the buffer and checks to decrease the size of intermediate data serialization buffer
	 * after finishing the whole serialization process including
	 * {@link #serializeRecord(IOReadableWritable)} and {@link #copyToBufferBuilder(BufferBuilder)}.
	 */
	void prune();

	/**
	 * Supports copying an intermediate data serialization buffer to multiple target buffers
	 * by resetting its initial position before each copying.
	 */
	void reset();

	/**
	 * @return <tt>true</tt> if has some serialized data pending copying to the result {@link BufferBuilder}.
	 */
	boolean hasSerializedData();
}
