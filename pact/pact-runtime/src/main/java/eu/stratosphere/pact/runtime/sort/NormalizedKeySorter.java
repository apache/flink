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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.services.iomanager.Writer;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessors;
import eu.stratosphere.pact.runtime.util.MathUtils;
import eu.stratosphere.pact.runtime.util.MemorySegmentListIterator;

/**
 * 
 * @author Stephan Ewen
 */
public final class NormalizedKeySorter<T> implements IndexedSortable
{
	
	private static final int OFFSET_LEN = 8;
	
	private static final int DEFAULT_MAX_NORMALIZED_KEY_LEN = 8;
	
	private static final int MIN_REQUIRED_BUFFERS = 3;

	// ------------------------------------------------------------------------
	//                               Members
	// ------------------------------------------------------------------------

	private final byte[] swapBuffer;
	
	private final TypeAccessors<T> accessors;
	
	private final T holder1, holder2;
	
	private final ArrayList<MemorySegment> freeMemory;
	
	private final Iterator<MemorySegment> memorySource;
	
	private final ArrayList<MemorySegment> sortIndex;
	
	private final ArrayList<MemorySegment> recordBuffers;
	
	private MemorySegment currentSortIndexSegment;
	
	private long currentDataBufferOffset;
	
	private int currentSortIndexOffset;
	
	private int numRecords;
	
	
	private final int numKeyBytes;
	
	private final int indexEntrySize;
	
	private final int segmentSizeMask;
	
	private final int indexEntriesPerSegment;
	
	private final int lastIndexEntryOffset;
	
	private final int segmentSizeBits;
	
	private final int totalNumBuffers;
	
	private final boolean normalizedKeyFullyDetermines;
	
	
	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	public NormalizedKeySorter(TypeAccessors<T> accessors, List<MemorySegment> memory)
	{
		this(accessors, memory, DEFAULT_MAX_NORMALIZED_KEY_LEN);
	}
	
	public NormalizedKeySorter(TypeAccessors<T> accessors, List<MemorySegment> memory, int maxNormalizedKeyBytes)
	{
		if (accessors == null || memory == null)
			throw new NullPointerException();
		if (maxNormalizedKeyBytes < 0)
			throw new IllegalArgumentException("Maximal number of normalized key bytes must not be negative.");
		
		this.accessors = accessors;
		this.holder1 = accessors.createInstance();
		this.holder2 = accessors.createInstance();
		
		// check the size of the first buffer and record it. all further buffers must have the same size.
		// the size must also be a power of 2
		this.totalNumBuffers = memory.size();
		if (this.totalNumBuffers < MIN_REQUIRED_BUFFERS) {
			throw new IllegalArgumentException("Normalized-Key sorter requires at least " + MIN_REQUIRED_BUFFERS + " memory buffers.");
		}
		final int segmentSize = memory.get(0).size();
		if ( (segmentSize & segmentSize - 1) != 0) {
			throw new IllegalArgumentException("Normalized-Key-Sort requires buffers whose size is a power of 2.");
		}
		this.segmentSizeMask = segmentSize - 1;
		this.segmentSizeBits = MathUtils.log2floor(segmentSize);
		
		if (memory instanceof ArrayList<?>) {
			this.freeMemory = (ArrayList<MemorySegment>) memory;
		}
		else {
			this.freeMemory = new ArrayList<MemorySegment>(memory.size());
			this.freeMemory.addAll(memory);
		}
		this.memorySource = new MemorySegmentListIterator(this.freeMemory);
		
		// create the buffer collections
		this.sortIndex = new ArrayList<MemorySegment>(16);
		this.recordBuffers = new ArrayList<MemorySegment>(16);
		
		// set up normalized key characteristics
		if (this.accessors.supportsNormalizedKey()) {
			this.numKeyBytes = Math.min(this.accessors.getNormalizeKeyLen(), maxNormalizedKeyBytes);
			this.normalizedKeyFullyDetermines = !this.accessors.isNormalizedKeyPrefixOnly(this.numKeyBytes);
		}
		else {
			this.numKeyBytes = 0;
			this.normalizedKeyFullyDetermines = false;
		}
		
		// compute the index entry size and limits
		this.indexEntrySize = this.numKeyBytes + OFFSET_LEN;
		this.indexEntriesPerSegment = segmentSize / this.indexEntrySize;
		this.lastIndexEntryOffset = (this.indexEntriesPerSegment - 1) * this.indexEntrySize;
		this.swapBuffer = new byte[this.indexEntrySize];
		
		// set to initial state
		reset();
	}

	// -------------------------------------------------------------------------
	// Memory Segment
	// -------------------------------------------------------------------------

	/**
	 * Resets the sort buffer back to the state where it is empty. All contained data is discarded.
	 */
	public void reset()
	{
		// reset all offsets
		this.numRecords = 0;
		this.currentSortIndexOffset = 0;
		this.currentDataBufferOffset = 0;
		
		// return all memory
		this.freeMemory.addAll(this.sortIndex);
		this.freeMemory.addAll(this.recordBuffers);
		this.sortIndex.clear();
		this.recordBuffers.clear();
		
		// grab first buffers
		this.currentSortIndexSegment = nextMemorySegment();
		this.sortIndex.add(this.currentSortIndexSegment);
		final MemorySegment dbs = nextMemorySegment();
		this.recordBuffers.add(dbs);
	}

	/**
	 * Checks whether the buffer is empty.
	 * 
	 * @return True, if no record is contained, false otherwise.
	 */
	public boolean isEmpty()
	{
		return this.numRecords == 0;
	}
	
	public List<MemorySegment> dispose() {
		this.freeMemory.addAll(this.sortIndex);
		this.freeMemory.addAll(this.recordBuffers);
		
		this.recordBuffers.clear();
		this.sortIndex.clear();
		
		return this.freeMemory;
	}

	// -------------------------------------------------------------------------
	// Retrieving and Writing
	// -------------------------------------------------------------------------

	public void getRecord(T target, int logicalPosition) throws IOException
	{
		getRecordFromBuffer(target, readDataBufferOffset(logicalPosition));
	}


	public boolean write(T record) throws IOException
	{
		//check whether we need a new memory segment for the sort index
		if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
			if (memoryAvailable()) {
				this.currentSortIndexSegment = nextMemorySegment();
				this.sortIndex.add(this.currentSortIndexSegment);
				this.currentSortIndexOffset = 0;
			}
			else return false;
		}
		
		// add the pointer and the normalized key
		this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, this.currentDataBufferOffset);
		this.accessors.putNormalizedKey(record, this.currentSortIndexSegment.getBackingArray(), this.currentSortIndexSegment.translateOffset(this.currentSortIndexOffset + OFFSET_LEN), this.numKeyBytes);
		
		// serialize the record into the data buffers
		long bytes = this.accessors.serialize(record, this.recordBuffers.get(this.recordBuffers.size() - 1).outputView, this.memorySource, this.recordBuffers);
		
		if (bytes >= 0) {
			this.currentSortIndexOffset += indexEntrySize;
			this.currentDataBufferOffset += bytes;
			this.numRecords++;
			return true;
		}
		else return false;
	}
	
	// ------------------------------------------------------------------------
	

	private final long readDataBufferOffset(int logicalPosition)
	{
		if (logicalPosition < 0 | logicalPosition >= this.numRecords) {
			throw new IndexOutOfBoundsException();
		}
		
		final int bufferNum = logicalPosition / this.indexEntriesPerSegment;
		final int segmentOffset = logicalPosition % this.indexEntriesPerSegment;
		
		return this.sortIndex.get(bufferNum).getLong(segmentOffset * this.indexEntrySize);
	}
	
	private final void getRecordFromBuffer(T target, long pointer) throws IOException
	{
		final int buffer = (int) (pointer >>> this.segmentSizeBits);
		final int offset = (int) (pointer & this.segmentSizeMask);
		this.accessors.deserialize(target, this.recordBuffers, buffer, offset);
	}
	
	private final int compareRecords(long pointer1, long pointer2)
	{
		final int bufferI = (int) (pointer1 >>> this.segmentSizeBits);
		final int offsetI = (int) (pointer1 & this.segmentSizeMask);
		final int bufferJ = (int) (pointer2 >>> this.segmentSizeBits);
		final int offsetJ = (int) (pointer2 & this.segmentSizeMask);
		return this.accessors.compare(this.holder1, this.holder2, this.recordBuffers, this.recordBuffers, bufferI, bufferJ, offsetI, offsetJ);
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
	
//	/**
//	 * Writes this buffer completely to the given writer.
//	 * 
//	 * @param writer The writer to write the segment to.
//	 * @throws IOException Thrown, if the writer caused an I/O exception.
//	 */
//	public void writeToChannel(final Writer writer) throws IOException {
//		if (!isBound()) {
//			new UnboundMemoryBackedException();
//		}
//
//		final MemoryIOWrapper memoryWrapper = new MemoryIOWrapper(this.memory);
//
//		// write according to index
//		for (int i = 0; i < size(); i++)
//		{
//			int offsetPosition = readOffsetPosition(i);
//
//			// start and end within memory segment
//			int kvstart = readPairOffset(offsetPosition);
//			int kvend = 0;
//			
//			// for the last pair there is no next pair
//			if(offsetPosition - STACK_ENTRY_SIZE > this.outputView.getStackEndRel()) {
//				// -> kvend = kvstart of next pair
//				kvend = readPairOffset(offsetPosition - STACK_ENTRY_SIZE);
//			}
//			else {
//				kvend = this.position;
//			}
//
//			// set offset within memory segment
//			final int kvlength = kvend - kvstart;
//			memoryWrapper.setIOBlock(kvstart, kvlength);
//
//			// copy serialized pair to writer
//			writer.write(memoryWrapper);
//		}
//	}

//	/**
//	 * Writes a series of key/value pairs in this buffer to the given writer.
//	 * 
//	 * @param writer The writer to write the pairs to.
//	 * @param start The position (logical number) of the first pair that is written.
//	 * @param num The number of pairs to be written.
//	 * @throws IOException Thrown, if the writer caused an I/O exception.
//	 */
//	public void writeToChannel(final Writer writer, final int start, final int num) throws IOException {
//		// write according to index
//		for (int i = start; i < start + num; i++) {
//			// offset to index element
//			int offsetPosition = readOffsetPosition(i);
//
//			// start and end within memory segment
//			int kvstart = readPairOffset(offsetPosition);
//			int kvend = 0;
//			
//			// for the last pair there is no next pair
//			if(offsetPosition - STACK_ENTRY_SIZE > this.outputView.getStackEndRel()) {
//				// -> kvend = kvstart of next pair
//				kvend = readPairOffset(offsetPosition - STACK_ENTRY_SIZE);
//			}
//			else {
//				kvend = this.position;
//			}
//
//			// set offset within memory segment
//			final int kvlength = kvend - kvstart;
//			memoryWrapper.setIOBlock(kvstart, kvlength);
//
//			// copy serialized pair to writer
//			writer.write(memoryWrapper);
//		}
//	}

	// -------------------------------------------------------------------------
	// Indexed Sorting
	// -------------------------------------------------------------------------

	public int compare(int i, int j)
	{
		final int bufferNumI = i / this.indexEntriesPerSegment;
		final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;
		
		final int bufferNumJ = j / this.indexEntriesPerSegment;
		final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;
		
		final MemorySegment segI = this.sortIndex.get(bufferNumI);
		final MemorySegment segJ = this.sortIndex.get(bufferNumJ);
		final byte[] bI = segI.getBackingArray();
		final byte[] bJ = segJ.getBackingArray();
		
		int val = 0;
		for (int pos = 0, posI = segI.translateOffset(segmentOffsetI + OFFSET_LEN), posJ = segJ.translateOffset(segmentOffsetJ + OFFSET_LEN);
			pos < this.numKeyBytes & (val = (bI[posI] & 0xff) - (bJ[posJ] & 0xff)) == 0; pos++, posI++, posJ++);
		
		if (val != 0 || this.normalizedKeyFullyDetermines) {
			return val;
		}
		
		final long pointerI = segI.getLong(segmentOffsetI);
		final long pointerJ = segJ.getLong(segmentOffsetJ);
		
		return compareRecords(pointerI, pointerJ);
	}

	@Override
	public void swap(int i, int j)
	{
		final int bufferNumI = i / this.indexEntriesPerSegment;
		final int segmentOffsetI = (i % this.indexEntriesPerSegment) * this.indexEntrySize;
		
		final int bufferNumJ = j / this.indexEntriesPerSegment;
		final int segmentOffsetJ = (j % this.indexEntriesPerSegment) * this.indexEntrySize;
		
		final MemorySegment segI = this.sortIndex.get(bufferNumI);
		final MemorySegment segJ = this.sortIndex.get(bufferNumJ);
		
		segI.get(segmentOffsetI, this.swapBuffer, 0, this.indexEntrySize);
		System.arraycopy(segJ.getBackingArray(), segJ.translateOffset(segmentOffsetJ), segI.getBackingArray(), segI.translateOffset(segmentOffsetI), this.indexEntrySize);
		segJ.put(segmentOffsetJ, this.swapBuffer, 0, this.indexEntrySize);
	}

	@Override
	public int size()
	{
		return this.numRecords;
	}

	// -------------------------------------------------------------------------
	
	/**
	 * @return
	 */
	public final MutableObjectIterator<T> getIterator()
	{
		return new MutableObjectIterator<T>()
		{
			private final int size = size();
			private int current = 0;
			
			private int currentSegment = 0;
			private int currentOffset = 0;
			
			private MemorySegment currentIndexSegment = sortIndex.get(0);

			@Override
			public boolean next(T target)
			{
				if (this.current < this.size) {
					this.current++;
					if (this.currentOffset > lastIndexEntryOffset) {
						this.currentOffset = 0;
						this.currentIndexSegment = sortIndex.get(++this.currentSegment);
					}
					
					long pointer = this.currentIndexSegment.getLong(this.currentOffset);
					this.currentOffset += indexEntrySize;
					
					try {
						getRecordFromBuffer(target, pointer);
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
	
	// ------------------------------------------------------------------------
	
	/**
	 * Writes this buffer completely to the given writer.
	 * 
	 * @param writer The writer to write the segment to.
	 * @throws IOException Thrown, if the writer caused an I/O exception.
	 */
	public void writeToChannel(final Writer writer) throws IOException
	{
		int recordsLeft = this.numRecords;
		int currentMemSeg = 0;
		while (recordsLeft > 0)
		{
			final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++);
			int offset = 0;
			// check whether we have a full or partially full segment
			if (recordsLeft >= this.indexEntriesPerSegment) {
				// full segment
				for (;offset <= this.lastIndexEntryOffset; offset += this.indexEntrySize) {
					final long pointer = currentIndexSegment.getLong(offset);
					
				}
				recordsLeft -= this.indexEntriesPerSegment;
			} else {
				// partially filled segment
				for (; recordsLeft > 0; recordsLeft--, offset += this.indexEntrySize)
				{
					final long pointer = currentIndexSegment.getLong(offset);
					
				}
			}
		}
	}
}
