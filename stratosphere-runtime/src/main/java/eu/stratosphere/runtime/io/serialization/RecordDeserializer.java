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

import java.io.IOException;

/**
 * Interface for turning sequences of memory segments into records.
 */
public interface RecordDeserializer<T extends IOReadableWritable> {

	public static enum DeserializationResult {
		PARTIAL_RECORD(false, true),
		INTERMEDIATE_RECORD_FROM_BUFFER(true, false),
		LAST_RECORD_FROM_BUFFER(true, true);

		private final boolean isFullRecord;

		private final boolean isBufferConsumed;

		private DeserializationResult(boolean isFullRecord, boolean isBufferConsumed) {
			this.isFullRecord = isFullRecord;
			this.isBufferConsumed = isBufferConsumed;
		}

		public boolean isFullRecord () {
			return this.isFullRecord;
		}

		public boolean isBufferConsumed() {
			return this.isBufferConsumed;
		}
	}
	
	DeserializationResult getNextRecord(T target) throws IOException;

	void setNextMemorySegment(MemorySegment segment, int numBytes) throws IOException;

	void clear();
	
	boolean hasUnfinishedData();
}
