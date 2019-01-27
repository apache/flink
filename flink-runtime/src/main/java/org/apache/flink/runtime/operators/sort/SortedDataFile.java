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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.ChannelBackendMutableObjectIterator;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;

import java.io.IOException;
import java.util.List;

/**
 * A sorted data file is an on-disk file who saves a list of records in order. The saved
 * record is written in by an sorted {@link InMemorySorter}.
 */
public interface SortedDataFile<T> {

	/** Gets the underlying write channel. */
	FileIOChannel getWriteChannel();

	/** Gets the path of the underlying file. */
	FileIOChannel.ID getChannelID();

	/** writes an record. */
	void writeRecord(T record) throws IOException;

	/** Copies an serialized record. */
	void copyRecord(DataInputView serializedRecord) throws IOException;

	/** Gets the bytes written so far. */
	long getBytesWritten() throws IOException;

	/** Marks the writing finished and close the underlying file. */
	void finishWriting() throws IOException;

	/** Gets an iterator to read the saved records. */
	ChannelBackendMutableObjectIterator<T> createReader(List<MemorySegment> readMemory) throws IOException;
}
