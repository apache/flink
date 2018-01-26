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
	 * Starts serializing and copying the given record to the target buffer
	 * (if available).
	 *
	 * @param record the record to serialize
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 */
	SerializationResult addRecord(T record) throws IOException;

	/**
	 * Sets a (next) target buffer to use and continues writing remaining data
	 * to it until it is full.
	 *
	 * @param bufferBuilder the new target buffer to use
	 * @return how much information was written to the target buffer and
	 *         whether this buffer is full
	 */
	SerializationResult setNextBufferBuilder(BufferBuilder bufferBuilder) throws IOException;

	/**
	 * Retrieves the current target buffer and sets its size to the actual
	 * number of written bytes.
	 *
	 * <p>After calling this method, a new target buffer is required to continue
	 * writing (see {@link #setNextBufferBuilder(BufferBuilder)}).
	 *
	 * @return the target buffer that was used
	 */
	Buffer getCurrentBuffer();

	/**
	 * Resets the target buffer to <tt>null</tt>.
	 *
	 * <p><strong>NOTE:</strong> After calling this method, <strong>a new target
	 * buffer is required to continue writing</strong> (see
	 * {@link #setNextBufferBuilder(BufferBuilder)}).</p>
	 */
	void clearCurrentBuffer();

	/**
	 * Resets the target buffer to <tt>null</tt> and resets internal state set
	 * up for the record to serialize.
	 *
	 * <p><strong>NOTE:</strong> After calling this method, a <strong>new record
	 * and a new target buffer is required to start writing again</strong>
	 * (see {@link #setNextBufferBuilder(BufferBuilder)}). If you want to continue
	 * with the current record, use {@link #clearCurrentBuffer()} instead.</p>
	 */
	void clear();

	/**
	 * Determines whether data is left, either in the current target buffer or
	 * in any internal state set up for the record to serialize.
	 *
	 * @return <tt>true</tt> if some data is present
	 */
	boolean hasData();
}
