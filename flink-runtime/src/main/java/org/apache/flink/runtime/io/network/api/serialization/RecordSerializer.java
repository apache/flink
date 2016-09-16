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

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.metrics.groups.IOMetricGroup;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.io.network.buffer.Buffer;

/**
 * Interface for turning records into sequences of memory segments.
 */
public interface RecordSerializer<T extends IOReadableWritable> {

	enum SerializationResult {
		PARTIAL_RECORD_MEMORY_SEGMENT_FULL(false, true),
		FULL_RECORD_MEMORY_SEGMENT_FULL(true, true),
		FULL_RECORD(true, false);
		
		private final boolean isFullRecord;

		private final boolean isFullBuffer;
		
		private SerializationResult(boolean isFullRecord, boolean isFullBuffer) {
			this.isFullRecord = isFullRecord;
			this.isFullBuffer = isFullBuffer;
		}
		
		public boolean isFullRecord() {
			return this.isFullRecord;
		}
		
		public boolean isFullBuffer() {
			return this.isFullBuffer;
		}
	}
	
	SerializationResult addRecord(T record) throws IOException;

	SerializationResult setNextBuffer(Buffer buffer) throws IOException;

	Buffer getCurrentBuffer();

	void clearCurrentBuffer();
	
	void clear();
	
	boolean hasData();

	/**
	 * Setter for the reporter, e.g. for the number of records emitted and the number of bytes read.
	 */
	void setReporter(AccumulatorRegistry.Reporter reporter);

	/**
	 * Insantiates all metrics.
	 *
	 * @param metrics metric group
	 */
	void instantiateMetrics(IOMetricGroup metrics);
}
