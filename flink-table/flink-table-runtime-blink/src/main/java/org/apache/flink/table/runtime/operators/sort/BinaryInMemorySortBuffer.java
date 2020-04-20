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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.typeutils.AbstractRowSerializer;
import org.apache.flink.table.runtime.typeutils.BinaryRowSerializer;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * In memory sort buffer for binary row.
 */
public final class BinaryInMemorySortBuffer extends BinaryIndexedSortable {

	private static final int MIN_REQUIRED_BUFFERS = 3;

	private AbstractRowSerializer<BaseRow> inputSerializer;
	private final ArrayList<MemorySegment> recordBufferSegments;
	private final SimpleCollectingOutputView recordCollector;
	private final int totalNumBuffers;

	private long currentDataBufferOffset;
	private long sortIndexBytes;

	/**
	 * Create a memory sorter in `insert` way.
	 */
	public static BinaryInMemorySortBuffer createBuffer(
			NormalizedKeyComputer normalizedKeyComputer,
			AbstractRowSerializer<BaseRow> inputSerializer,
			BinaryRowSerializer serializer,
			RecordComparator comparator,
			MemorySegmentPool memoryPool) {
		checkArgument(memoryPool.freePages() >= MIN_REQUIRED_BUFFERS);
		int totalNumBuffers = memoryPool.freePages();
		ArrayList<MemorySegment> recordBufferSegments = new ArrayList<>(16);
		return new BinaryInMemorySortBuffer(
				normalizedKeyComputer, inputSerializer, serializer, comparator, recordBufferSegments,
				new SimpleCollectingOutputView(recordBufferSegments, memoryPool, memoryPool.pageSize()),
				memoryPool, totalNumBuffers);
	}

	private BinaryInMemorySortBuffer(
			NormalizedKeyComputer normalizedKeyComputer,
			AbstractRowSerializer<BaseRow> inputSerializer,
			BinaryRowSerializer serializer,
			RecordComparator comparator,
			ArrayList<MemorySegment> recordBufferSegments,
			SimpleCollectingOutputView recordCollector,
			MemorySegmentPool pool,
			int totalNumBuffers) {
		super(normalizedKeyComputer, serializer, comparator, recordBufferSegments, pool);
		this.inputSerializer = inputSerializer;
		this.recordBufferSegments = recordBufferSegments;
		this.recordCollector = recordCollector;
		this.totalNumBuffers = totalNumBuffers;
	}

	// -------------------------------------------------------------------------
	// Memory Segment
	// -------------------------------------------------------------------------

	/**
	 * Resets the sort buffer back to the state where it is empty. All contained data is discarded.
	 */
	public void reset() {

		// reset all offsets
		this.numRecords = 0;
		this.currentSortIndexOffset = 0;
		this.currentDataBufferOffset = 0;
		this.sortIndexBytes = 0;

		// return all memory
		returnToSegmentPool();

		// grab first buffers
		this.currentSortIndexSegment = nextMemorySegment();
		this.sortIndex.add(this.currentSortIndexSegment);
		this.recordCollector.reset();
	}

	public void returnToSegmentPool() {
		// return all memory
		this.memorySegmentPool.returnAll(this.sortIndex);
		this.memorySegmentPool.returnAll(this.recordBufferSegments);
		this.sortIndex.clear();
		this.recordBufferSegments.clear();
	}

	/**
	 * Checks whether the buffer is empty.
	 *
	 * @return True, if no record is contained, false otherwise.
	 */
	public boolean isEmpty() {
		return this.numRecords == 0;
	}

	public void dispose() {
		returnToSegmentPool();
	}

	public long getCapacity() {
		return ((long) this.totalNumBuffers) * memorySegmentPool.pageSize();
	}

	public long getOccupancy() {
		return this.currentDataBufferOffset + this.sortIndexBytes;
	}

	/**
	 * Writes a given record to this sort buffer. The written record will be appended and take
	 * the last logical position.
	 *
	 * @param record The record to be written.
	 * @return True, if the record was successfully written, false, if the sort buffer was full.
	 * @throws IOException Thrown, if an error occurred while serializing the record into the buffers.
	 */
	public boolean write(BaseRow record) throws IOException {
		//check whether we need a new memory segment for the sort index
		if (!checkNextIndexOffset()) {
			return false;
		}

		// serialize the record into the data buffers
		int skip;
		try {
			skip = this.inputSerializer.serializeToPages(record, this.recordCollector);
		} catch (EOFException e) {
			return false;
		}

		final long newOffset = this.recordCollector.getCurrentOffset();
		long currOffset = currentDataBufferOffset + skip;

		writeIndexAndNormalizedKey(record, currOffset);

		this.currentDataBufferOffset = newOffset;

		return true;
	}

	private BinaryRow getRecordFromBuffer(BinaryRow reuse, long pointer) throws IOException {
		this.recordBuffer.setReadPosition(pointer);
		return this.serializer.mapFromPages(reuse, this.recordBuffer);
	}

	// -------------------------------------------------------------------------

	/**
	 * Gets an iterator over all records in this buffer in their logical order.
	 *
	 * @return An iterator returning the records in their logical order.
	 */
	public final MutableObjectIterator<BinaryRow> getIterator() {
		return new MutableObjectIterator<BinaryRow>() {
			private final int size = size();
			private int current = 0;

			private int currentSegment = 0;
			private int currentOffset = 0;

			private MemorySegment currentIndexSegment = sortIndex.get(0);

			@Override
			public BinaryRow next(BinaryRow target) {
				if (this.current < this.size) {
					this.current++;
					if (this.currentOffset > lastIndexEntryOffset) {
						this.currentOffset = 0;
						this.currentIndexSegment = sortIndex.get(++this.currentSegment);
					}

					long pointer = this.currentIndexSegment.getLong(this.currentOffset);
					this.currentOffset += indexEntrySize;

					try {
						return getRecordFromBuffer(target, pointer);
					} catch (IOException ioe) {
						throw new RuntimeException(ioe);
					}
				} else {
					return null;
				}
			}

			@Override
			public BinaryRow next() {
				throw new RuntimeException("Not support!");
			}
		};
	}
}
