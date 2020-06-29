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

package org.apache.flink.connector.file.src.util;

/**
 * A record with its position in the file
 *
 * <p>The position defines the point in the file after the record, so it is the position where the
 * reading resumes if there is a failure right after this record was handled.
 *
 * <p>This class is immutable for safety. Use {@link MutableRecordAndPosition} if you need a mutable
 * version for efficiency.
 */
public class RecordAndPosition<E> {

	// these are package private and non-final so we can mutate them from the MutableRecordAndPosition
	E record;
	long offset;
	long recordSkipCount;

	/**
	 * Creates a new {@code RecordAndPosition} with the given record and position info.
	 */
	public RecordAndPosition(E record, long offset, long recordSkipCount) {
		this.record = record;
		this.offset = offset;
		this.recordSkipCount = recordSkipCount;
	}

	/**
	 * Package private constructor for the {@link MutableRecordAndPosition}.
	 */
	RecordAndPosition() {}

	// ------------------------------------------------------------------------

	public E getRecord() {
		return record;
	}

	public long getOffset() {
		return offset;
	}

	public long getRecordSkipCount() {
		return recordSkipCount;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("%s @ %d + %d", record, offset, recordSkipCount);
	}
}
