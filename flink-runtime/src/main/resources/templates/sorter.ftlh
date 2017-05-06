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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.SimpleCollectingOutputView;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;
import org.apache.flink.util.MutableObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.sort.LargeRecordHandler;

/**
 * This code is generated on ${.now}
 */
public final class ${name}<T> extends NormalizedKeySorter<T> {

	private static final Logger LOG = LoggerFactory.getLogger(${name}.class);

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	public ${name}(TypeSerializer<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory) {
		super(serializer, comparator, memory);
	}

	public ${name}(TypeSerializer<T> serializer, TypeComparator<T> comparator,
			List<MemorySegment> memory, int maxNormalizedKeyBytes)
	{
		super(serializer, comparator, memory, maxNormalizedKeyBytes);
	}

	/**
	 * Writes a given record to this sort buffer. The written record will be appended and take
	 * the last logical position.
	 *
	 * @param record The record to be written.
	 * @return True, if the record was successfully written, false, if the sort buffer was full.
	 * @throws IOException Thrown, if an error occurred while serializing the record into the buffers.
	 */

	@Override
	public boolean write(T record) throws IOException {
		//check whether we need a new memory segment for the sort index
		if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
			if (memoryAvailable()) {
				this.currentSortIndexSegment = nextMemorySegment();
				this.sortIndex.add(this.currentSortIndexSegment);
				this.currentSortIndexOffset = 0;
				this.sortIndexBytes += this.segmentSize;
			} else {
				return false;
			}
		}

		// serialize the record into the data buffers
		try {
			this.serializer.serialize(record, this.recordCollector);
		}
		catch (EOFException e) {
			return false;
		}

		final long newOffset = this.recordCollector.getCurrentOffset();
		final boolean shortRecord = newOffset - this.currentDataBufferOffset < LARGE_RECORD_THRESHOLD;

		if (!shortRecord && LOG.isDebugEnabled()) {
			LOG.debug("Put a large record ( >" + LARGE_RECORD_THRESHOLD + " into the sort buffer");
		}

		// add the pointer and the normalized key
		this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, shortRecord ?
				this.currentDataBufferOffset : (this.currentDataBufferOffset | LARGE_RECORD_TAG));

		if (this.numKeyBytes != 0) {
			this.comparator.putNormalizedKey(record, this.currentSortIndexSegment, this.currentSortIndexOffset + OFFSET_LEN, this.numKeyBytes);
		 }

// generated code
${writeProcedures}
// end

		this.currentSortIndexOffset += this.indexEntrySize;
		this.currentDataBufferOffset = newOffset;
		this.numRecords++;
		return true;
	}


	// -------------------------------------------------------------------------
	// Indexed Sorting
	// -------------------------------------------------------------------------

	@Override
	public int compare(int i, int j) {
		final int bufferNumI = i / this.indexEntriesPerSegment;
		final int iBufferOffset = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

		final int bufferNumJ = j / this.indexEntriesPerSegment;
		final int jBufferOffset = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

		final MemorySegment segI = (MemorySegment)this.sortIndex.get(bufferNumI);
		final MemorySegment segJ = (MemorySegment)this.sortIndex.get(bufferNumJ);

// generated code
${compareProcedures}
// end
	}

	@Override
	public int compare(int iBufferNumber, int iBufferOffset, int jBufferNumber, int jBufferOffset) {
		final MemorySegment segI = (MemorySegment)this.sortIndex.get(iBufferNumber);
		final MemorySegment segJ = (MemorySegment)this.sortIndex.get(jBufferNumber);

// generated code
${compareProcedures}
// end

	}

	@Override
	public void swap(int i, int j) {
		final int bufferNumI = i / this.indexEntriesPerSegment;
		final int iBufferOffset = (i % this.indexEntriesPerSegment) * this.indexEntrySize;

		final int bufferNumJ = j / this.indexEntriesPerSegment;
		final int jBufferOffset = (j % this.indexEntriesPerSegment) * this.indexEntrySize;

		final MemorySegment segI = (MemorySegment)this.sortIndex.get(bufferNumI);
		final MemorySegment segJ = (MemorySegment)this.sortIndex.get(bufferNumJ);

// generated code
${swapProcedures}
// end
	}

	@Override
	public void swap(int iBufferNumber, int iBufferOffset, int jBufferNumber, int jBufferOffset) {
		final MemorySegment segI = (MemorySegment)this.sortIndex.get(iBufferNumber);
		final MemorySegment segJ = (MemorySegment)this.sortIndex.get(jBufferNumber);

// generated code
${swapProcedures}
// end

	}

	@Override
	public String toString(){
	    return "${name}";
	}
}

