/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.generated.NormalizedKeyComputer;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.typeutils.BinaryRowSerializer;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * In memory KV sortable buffer for binary row, it already has records in memory.
 */
public class BinaryKVInMemorySortBuffer extends BinaryIndexedSortable {

	private final BinaryRowSerializer valueSerializer;

	public static BinaryKVInMemorySortBuffer createBuffer(
			NormalizedKeyComputer normalizedKeyComputer,
			BinaryRowSerializer keySerializer,
			BinaryRowSerializer valueSerializer,
			RecordComparator comparator,
			ArrayList<MemorySegment> recordBufferSegments,
			long numElements,
			MemorySegmentPool pool) throws IOException {
		BinaryKVInMemorySortBuffer sorter = new BinaryKVInMemorySortBuffer(
				normalizedKeyComputer, keySerializer, valueSerializer, comparator,
				recordBufferSegments, pool);
		sorter.load(numElements, sorter.recordBuffer);
		return sorter;
	}

	private BinaryKVInMemorySortBuffer(
			NormalizedKeyComputer normalizedKeyComputer,
			BinaryRowSerializer keySerializer,
			BinaryRowSerializer valueSerializer,
			RecordComparator comparator,
			ArrayList<MemorySegment> recordBufferSegments,
			MemorySegmentPool memorySegmentPool) throws IOException {
		super(normalizedKeyComputer, keySerializer, comparator, recordBufferSegments, memorySegmentPool);
		this.valueSerializer = valueSerializer;
	}

	@Override
	public void writeToOutput(AbstractPagedOutputView output) throws IOException {
		final int numRecords = this.numRecords;
		int currentMemSeg = 0;
		int currentRecord = 0;

		while (currentRecord < numRecords) {
			final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++);

			// go through all records in the memory segment
			for (int offset = 0; currentRecord < numRecords && offset <= this.lastIndexEntryOffset; currentRecord++,
					offset += this.indexEntrySize) {
				final long pointer = currentIndexSegment.getLong(offset);
				this.recordBuffer.setReadPosition(pointer);
				this.serializer.copyFromPagesToView(this.recordBuffer, output);
				this.valueSerializer.copyFromPagesToView(this.recordBuffer, output);
			}
		}
	}

	private void load(long numElements, RandomAccessInputView recordInputView) throws IOException {
		for (int index = 0; index < numElements; index++) {
			serializer.checkSkipReadForFixLengthPart(recordInputView);
			long pointer = recordInputView.getReadPosition();
			BinaryRow row = serializer1.mapFromPages(row1, recordInputView);
			valueSerializer.checkSkipReadForFixLengthPart(recordInputView);
			recordInputView.skipBytes(recordInputView.readInt());
			boolean success = checkNextIndexOffset();
			checkArgument(success);
			writeIndexAndNormalizedKey(row, pointer);
		}
	}
}
