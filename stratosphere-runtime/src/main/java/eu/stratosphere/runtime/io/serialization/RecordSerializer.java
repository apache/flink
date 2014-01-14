/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.serialization;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.runtime.io.Buffer;

import java.io.IOException;

/**
 * Interface for turning records into sequences of memory segments.
 */
public interface RecordSerializer<T extends IOReadableWritable> {

	public static enum SerializationResult {
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
	
	void clear();
	
	boolean hasData();
}
