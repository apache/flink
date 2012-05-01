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
import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.ListMemorySegmentSource;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.io.ChannelWriterOutputView;
import eu.stratosphere.pact.runtime.io.RandomAccessInputView;
import eu.stratosphere.pact.runtime.io.SimpleCollectingOutputView;
import eu.stratosphere.pact.runtime.plugable.TypeComparator;
import eu.stratosphere.pact.runtime.plugable.TypeSerializers;

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
	
	private final TypeSerializers<T> serializer;
	
	private final TypeComparator<T> comparator;
	
	private final SimpleCollectingOutputView recordCollector;
	
	private final RandomAccessInputView recordBuffer;
	
	private final RandomAccessInputView recordBufferForComparison;
	
	private MemorySegment currentSortIndexSegment;
	
	private final ArrayList<MemorySegment> freeMemory;
	
	private final ArrayList<MemorySegment> sortIndex;
	
	private final ArrayList<MemorySegment> recordBufferSegments;
	
	private long currentDataBufferOffset;
	
	private long sortIndexBytes;
	
	private int currentSortIndexOffset;
	
	private int numRecords;
	
	private final int numKeyBytes;
	
	private final int indexEntrySize;
	
	private final int indexEntriesPerSegment;
	
	private final int lastIndexEntryOffset;
	
	private final int segmentSize;
	
	private final int totalNumBuffers;
	
	private final boolean normalizedKeyFullyDetermines;
	
	
	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	public NormalizedKeySorter(TypeSerializers<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory)
	{
		this(serializer, comparator, memory, DEFAULT_MAX_NORMALIZED_KEY_LEN);
	}
	
	public NormalizedKeySorter(TypeSerializers<T> serializer, TypeComparator<T> comparator, 
			List<MemorySegment> memory, int maxNormalizedKeyBytes)
	{
		if (serializer == null || comparator == null || memory == null)
			throw new NullPointerException();
		if (maxNormalizedKeyBytes < 0)
			throw new IllegalArgumentException("Maximal number of normalized key bytes must not be negative.");
		
		this.serializer = serializer;
		this.comparator = comparator;
		
		// check the size of the first buffer and record it. all further buffers must have the same size.
		// the size must also be a power of 2
		this.totalNumBuffers = memory.size();
		if (this.totalNumBuffers < MIN_REQUIRED_BUFFERS) {
			throw new IllegalArgumentException("Normalized-Key sorter requires at least " + MIN_REQUIRED_BUFFERS + " memory buffers.");
		}
		this.segmentSize = memory.get(0).size();
		
		if (memory instanceof ArrayList<?>) {
			this.freeMemory = (ArrayList<MemorySegment>) memory;
		}
		else {
			this.freeMemory = new ArrayList<MemorySegment>(memory.size());
			this.freeMemory.addAll(memory);
		}
		
		// create the buffer collections
		this.sortIndex = new ArrayList<MemorySegment>(16);
		this.recordBufferSegments = new ArrayList<MemorySegment>(16);
		
		// the views for the record collections
		this.recordCollector = new SimpleCollectingOutputView(this.recordBufferSegments,
			new ListMemorySegmentSource(this.freeMemory), this.segmentSize);
		this.recordBuffer = new RandomAccessInputView(this.recordBufferSegments, this.segmentSize);
		this.recordBufferForComparison = new RandomAccessInputView(this.recordBufferSegments, this.segmentSize);
		
		// set up normalized key characteristics
		if (this.comparator.supportsNormalizedKey()) {
			this.numKeyBytes = Math.min(this.comparator.getNormalizeKeyLen(), maxNormalizedKeyBytes);
			this.normalizedKeyFullyDetermines = !this.comparator.isNormalizedKeyPrefixOnly(this.numKeyBytes);
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
		this.currentSortIndexSegment = nextMemorySegment();
		this.sortIndex.add(this.currentSortIndexSegment);
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
		this.sortIndexBytes = 0;
		
		// return all memory
		this.freeMemory.addAll(this.sortIndex);
		this.freeMemory.addAll(this.recordBufferSegments);
		this.sortIndex.clear();
		this.recordBufferSegments.clear();
		
		// grab first buffers
		this.currentSortIndexSegment = nextMemorySegment();
		this.sortIndex.add(this.currentSortIndexSegment);
		this.recordCollector.reset();
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
	
	/**
	 * Collects all memory segments from this sorter.
	 * 
	 * @return All memory segments from this sorter.
	 */
	public List<MemorySegment> dispose()
	{
		this.freeMemory.addAll(this.sortIndex);
		this.freeMemory.addAll(this.recordBufferSegments);
		
		this.recordBufferSegments.clear();
		this.sortIndex.clear();
		
		return this.freeMemory;
	}
	
	/**
	 * Gets the total capacity of this sorter, in bytes.
	 * 
	 * @return The sorter's total capacity.
	 */
	public long getCapacity()
	{
		return ((long) this.totalNumBuffers) * this.segmentSize;
	}
	
	/**
	 * Gets the number of bytes currently occupied in this sorter.
	 * 
	 * @return The number of bytes occupied.
	 */
	public long getOccupancy()
	{
		return this.currentDataBufferOffset + this.sortIndexBytes;
	}

	// -------------------------------------------------------------------------
	// Retrieving and Writing
	// -------------------------------------------------------------------------

	/**
	 * Gets the record at the given logical position.
	 * 
	 * @param target The target object to deserialize the record into.
	 * @param logicalPosition The logical position of the record.
	 * @throws IOException Thrown, if an exception occurred during deserialization.
	 */
	public void getRecord(T target, int logicalPosition) throws IOException
	{
		getRecordFromBuffer(target, readPointer(logicalPosition));
	}

	/**
	 * Writes a given record to this sort buffer. The written record will be appended and take
	 * the last logical position.
	 * 
	 * @param record The record to be written.
	 * @return True, if the record was successfully written, false, if the sort buffer was full.
	 * @throws IOException Thrown, if an error occurred while serializing the record into the buffers.
	 */
	public boolean write(T record) throws IOException
	{
		//check whether we need a new memory segment for the sort index
		if (this.currentSortIndexOffset > this.lastIndexEntryOffset) {
			if (memoryAvailable()) {
				this.currentSortIndexSegment = nextMemorySegment();
				this.sortIndex.add(this.currentSortIndexSegment);
				this.currentSortIndexOffset = 0;
				this.sortIndexBytes += this.segmentSize;
			}
			else return false;
		}
		
		// add the pointer and the normalized key
		this.currentSortIndexSegment.putLong(this.currentSortIndexOffset, this.currentDataBufferOffset);
		this.comparator.putNormalizedKey(record, this.currentSortIndexSegment.getBackingArray(), this.currentSortIndexSegment.translateOffset(this.currentSortIndexOffset + OFFSET_LEN), this.numKeyBytes);
		
		// serialize the record into the data buffers
		try {
			long bytes = this.serializer.serialize(record, this.recordCollector);
			this.currentSortIndexOffset += this.indexEntrySize;
			this.currentDataBufferOffset += bytes;
			this.numRecords++;
			return true;
		} catch (EOFException eofex) {
			return false;
		}
	}
	
	// ------------------------------------------------------------------------
	//                           Access Utilities
	// ------------------------------------------------------------------------
	
	private final long readPointer(int logicalPosition)
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
		this.recordBuffer.setReadPosition(pointer);
		this.serializer.deserialize(target, this.recordBuffer);
	}
	
	private final int compareRecords(long pointer1, long pointer2)
	{
		this.recordBuffer.setReadPosition(pointer1);
		this.recordBufferForComparison.setReadPosition(pointer2);
		
		try {
			return this.comparator.compare(this.recordBuffer, this.recordBufferForComparison);
		} catch (IOException ioex) {
			throw new RuntimeException("Error comparing two records.", ioex);
		}
	}
	
	private final boolean memoryAvailable()
	{
		return !this.freeMemory.isEmpty();
	}
	
	private final MemorySegment nextMemorySegment()
	{
		return this.freeMemory.remove(this.freeMemory.size() - 1);
	}

	// -------------------------------------------------------------------------
	// Indexed Sorting
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.IndexedSortable#compare(int, int)
	 */
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
			pos < this.numKeyBytes && (val = (bI[posI] & 0xff) - (bJ[posJ] & 0xff)) == 0; pos++, posI++, posJ++);
		
		if (val != 0 || this.normalizedKeyFullyDetermines) {
			return val;
		}
		
		final long pointerI = segI.getLong(segmentOffsetI);
		final long pointerJ = segJ.getLong(segmentOffsetJ);
		
		return compareRecords(pointerI, pointerJ);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.IndexedSortable#swap(int, int)
	 */
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

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.IndexedSortable#size()
	 */
	@Override
	public int size()
	{
		return this.numRecords;
	}

	// -------------------------------------------------------------------------
	
	/**
	 * Gets an iterator over all records in this buffer in their logical order.
	 * 
	 * @return An iterator returning the records in their logical order.
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
	//                Writing to a DataOutputView
	// ------------------------------------------------------------------------
	
	/**
	 * Writes the records in this buffer in their logical order to the given output.
	 * 
	 * @param output The output view to write the records to.
	 * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
	 */
	public void writeToOutput(final ChannelWriterOutputView output) throws IOException
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
					this.recordBuffer.setReadPosition(pointer);
					this.serializer.copy(this.recordBuffer, output);
					
				}
				recordsLeft -= this.indexEntriesPerSegment;
			} else {
				// partially filled segment
				for (; recordsLeft > 0; recordsLeft--, offset += this.indexEntrySize)
				{
					final long pointer = currentIndexSegment.getLong(offset);
					this.recordBuffer.setReadPosition(pointer);
					this.serializer.copy(this.recordBuffer, output);
				}
			}
		}
	}
	
	/**
	 * Writes a subset of the records in this buffer in their logical order to the given output.
	 * 
	 * @param output The output view to write the records to.
	 * @param start The logical start position of the subset.
	 * @param len The number of elements to write.
	 * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
	 */
	public void writeToOutput(final ChannelWriterOutputView output, final int start, int num) throws IOException
	{
		int currentMemSeg = start / this.indexEntriesPerSegment;
		int offset = (start % this.indexEntriesPerSegment) * this.indexEntrySize;
		
		while (num > 0)
		{
			final MemorySegment currentIndexSegment = this.sortIndex.get(currentMemSeg++);
			// check whether we have a full or partially full segment
			if (num >= this.indexEntriesPerSegment && offset == 0) {
				// full segment
				for (;offset <= this.lastIndexEntryOffset; offset += this.indexEntrySize) {
					final long pointer = currentIndexSegment.getLong(offset);
					this.recordBuffer.setReadPosition(pointer);
					this.serializer.copy(this.recordBuffer, output);
				}
				num -= this.indexEntriesPerSegment;
			} else {
				// partially filled segment
				for (; num > 0 && offset <= this.lastIndexEntryOffset; num--, offset += this.indexEntrySize)
				{
					final long pointer = currentIndexSegment.getLong(offset);
					this.recordBuffer.setReadPosition(pointer);
					this.serializer.copy(this.recordBuffer, output);
				}
			}
			offset = 0;
		}
	}
}
