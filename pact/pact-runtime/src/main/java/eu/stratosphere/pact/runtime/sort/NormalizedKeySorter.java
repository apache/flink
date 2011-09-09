/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.sort;

import java.io.EOFException;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.services.iomanager.MemoryIOWrapper;
import eu.stratosphere.nephele.services.iomanager.RawComparator;
import eu.stratosphere.nephele.services.iomanager.Writer;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemoryBacked;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.UnboundMemoryBackedException;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultDataOutputView;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemorySegmentView;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.plugable.TypeAccessors;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;

/**
 * 
 * @author Stephan Ewen
 */
public final class NormalizedKeySorter<T> implements IndexedSortable
{
	
	private static final int OFFSET_LEN = 8;
	
	private static final int MAX_NORMALIZED_KEY_LEN = 16;

	// ------------------------------------------------------------------------
	//                               Members
	// ------------------------------------------------------------------------

	private final TypeAccessors<T> accessors;
	
	private final ArrayList<MemorySegment> freeMemory;
	
	private final ArrayList<MemorySegment> sortIndex;
	
	private final ArrayList<MemorySegment> recordBuffers;
	
	private MemorySegment currentSortIndexSegment;
	
	private DataOutputView currentDataBufferSegment;
	
	private long currentDataBufferOffset;
	
	private int currentSortIndexOffset;
	
	private int numRecords;
	
	
	private final int numKeyBytes;
	
	private final int indexEntrySize;
	
	private final int segmentSize;
	
	private final int indexEntriesPerSegment;
	
	private final int lastIndexEntryOffset;
	
	
	
	
	
	
	
	
	
//	private final MemoryIOWrapper memoryWrapper;
//
//	private final RawComparator comparator;
//	
//	
//	
//	
//	private int position;
	
	
	
	
	private final int totalNumBuffers;

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	public NormalizedKeySorter(TypeAccessors<T> accessors, List<MemorySegment> memory)
	{
		if (this.accessors == null) {
			throw new NullPointerException();
		}
		this.accessors = accessors;
		
		// check the size of the first buffer and record it. all further buffers must have the same size.
		// the size must also be a power of 2
		this.totalNumBuffers = memory.size();
		this.segmentSize = memory.get(0).size();
		if ( (this.segmentSize & this.segmentSize - 1) != 0) {
			throw new IllegalArgumentException("Normalized-Key-Sort requires buffers whose size is a power of 2.");
		}
		if (memory instanceof ArrayList<?>) {
			this.freeMemory = (ArrayList<MemorySegment>) memory;
		}
		else {
			this.freeMemory = new ArrayList<MemorySegment>(memory.size());
			this.freeMemory.addAll(memory);
		}
		
		this.sortIndex = new ArrayList<MemorySegment>(16);
		this.recordBuffers = new ArrayList<MemorySegment>(16);
		
		this.numKeyBytes = this.accessors.getNormalizeKeyLen();
		this.indexEntrySize = this.numKeyBytes + OFFSET_LEN;
		this.indexEntriesPerSegment = this.segmentSize / this.indexEntrySize;
		this.lastIndexEntryOffset = (this.indexEntriesPerSegment - 1) * this.indexEntrySize;
	}

	// -------------------------------------------------------------------------
	// Memory Segment
	// -------------------------------------------------------------------------


	public void reset()
	{
		this.numRecords = 0;
	}

	// -------------------------------------------------------------------------
	// Buffering
	// -------------------------------------------------------------------------

	public boolean isEmpty() {
		return this.numRecords == 0;
	}

	// -------------------------------------------------------------------------
	// Retrieving and Writing
	// -------------------------------------------------------------------------

	public void getRecord(PactRecord target, int logicalPosition) throws IOException
	{
		final int physicalPosition = readOffsetPosition(logicalPosition);
		final int start = readPairOffset(physicalPosition);
		this.memory.inputView.setPosition(start);
		
		target.read(this.memory.inputView);
	}


	public boolean write(T record)
	{
		if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
			// get a new memory segment for the sort index
			if (memoryAvailable()) {
				this.currentSortIndexSegment = nextMemorySegment();
				this.sortIndex.add(this.currentSortIndexSegment);
			}
			else return false;
		}
		
		this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, this.currentDataBufferOffset);
		this.accessors.putNormalizedKey(this.currentSortIndexSegment.getBackingArray(), this.currentSortIndexSegment.translateOffset(this.currentSortIndexOffset + OFFSET_LEN));
		
		long bytes = this.accessors.serialize(record, this.currentDataBufferSegment, this.freeMemory, this.recordBuffers);
		if (bytes >= 0) {
			this.currentDataBufferOffset += bytes;
			this.currentDataBufferSegment = null; // TODO
			this.numRecords++;
			return true;
		}
		else {
			return false;
		}
	}
	
	// ------------------------------------------------------------------------
	
	private final int readPairOffset(int physicalOffsetPosition)
	{
		return this.memory.getInt(physicalOffsetPosition);
	}
	
	private final void writeOffsetPosition(int logicalPosition, int offset)
	{
		final int stackoffset = (logicalPosition + 1) * STACK_ENTRY_SIZE;
		final int memoryoffset = this.outputView.getSize() - stackoffset;
		this.memory.putInt(memoryoffset, offset);
	}
	
	private final int readOffsetPosition(int logicalPosition)
	{
		final int stackoffset = (logicalPosition + 1) * STACK_ENTRY_SIZE;
		final int memoryoffset = this.outputView.getSize() - stackoffset;
		return this.memory.getInt(memoryoffset);
	}
	
	private final boolean memoryAvailable()
	{
		return !this.freeMemory.isEmpty();
	}
	private final MemorySegment nextMemorySegment()
	{
		return this.freeMemory.remove(this.freeMemory.size() - 1);
	}

	// ------------------------------------------------------------------------
	
	/**
	 * Writes this buffer completely to the given writer.
	 * 
	 * @param writer The writer to write the segment to.
	 * @throws IOException Thrown, if the writer caused an I/O exception.
	 */
	public void writeToChannel(final Writer writer) throws IOException {
		if (!isBound()) {
			new UnboundMemoryBackedException();
		}

		final MemoryIOWrapper memoryWrapper = new MemoryIOWrapper(this.memory);

		// write according to index
		for (int i = 0; i < size(); i++)
		{
			int offsetPosition = readOffsetPosition(i);

			// start and end within memory segment
			int kvstart = readPairOffset(offsetPosition);
			int kvend = 0;
			
			// for the last pair there is no next pair
			if(offsetPosition - STACK_ENTRY_SIZE > this.outputView.getStackEndRel()) {
				// -> kvend = kvstart of next pair
				kvend = readPairOffset(offsetPosition - STACK_ENTRY_SIZE);
			}
			else {
				kvend = this.position;
			}

			// set offset within memory segment
			final int kvlength = kvend - kvstart;
			memoryWrapper.setIOBlock(kvstart, kvlength);

			// copy serialized pair to writer
			writer.write(memoryWrapper);
		}
	}

	/**
	 * Writes a series of key/value pairs in this buffer to the given writer.
	 * 
	 * @param writer The writer to write the pairs to.
	 * @param start The position (logical number) of the first pair that is written.
	 * @param num The number of pairs to be written.
	 * @throws IOException Thrown, if the writer caused an I/O exception.
	 */
	public void writeToChannel(final Writer writer, final int start, final int num) throws IOException {
		// write according to index
		for (int i = start; i < start + num; i++) {
			// offset to index element
			int offsetPosition = readOffsetPosition(i);

			// start and end within memory segment
			int kvstart = readPairOffset(offsetPosition);
			int kvend = 0;
			
			// for the last pair there is no next pair
			if(offsetPosition - STACK_ENTRY_SIZE > this.outputView.getStackEndRel()) {
				// -> kvend = kvstart of next pair
				kvend = readPairOffset(offsetPosition - STACK_ENTRY_SIZE);
			}
			else {
				kvend = this.position;
			}

			// set offset within memory segment
			final int kvlength = kvend - kvstart;
			memoryWrapper.setIOBlock(kvstart, kvlength);

			// copy serialized pair to writer
			writer.write(memoryWrapper);
		}
	}

	// -------------------------------------------------------------------------
	// Indexed Sorting
	// -------------------------------------------------------------------------

	@Override
	public int compare(int i, int j)
	{
		final byte[] backingArray = this.memory.getBackingArray();
		
		// offsets into index
		final int offsetPositionI = readOffsetPosition(i);
		final int offsetPositionJ = readOffsetPosition(j);
		
		// starts of keys
		final int indexI = readPairOffset(offsetPositionI);
		final int indexJ = readPairOffset(offsetPositionJ);
		
		return comparator.compare(backingArray, backingArray, 
			this.memory.translateOffset(indexI),
			this.memory.translateOffset(indexJ));
	}

	@Override
	public void swap(int i, int j) {
		int offseti = readOffsetPosition(i);
		int offsetj = readOffsetPosition(j);
		writeOffsetPosition(i, offsetj);
		writeOffsetPosition(j, offseti);
	}

	@Override
	public int size()
	{
		return this.numRecords;
	}

	public final MutableObjectIterator<PactRecord> getIterator()
	{
		return new MutableObjectIterator<PactRecord>() {
			private final int size = size();
			private int current = 0;

			@Override
			public boolean next(PactRecord target) {
				if (this.current < this.size) {
					try {
						getRecord(target, this.current++);
						return true;
					}
					catch (IOException ioe) {
						throw new RuntimeException(ioe);
					}
				}
				else {
					return false;
				}
			}
		};
	}
}
