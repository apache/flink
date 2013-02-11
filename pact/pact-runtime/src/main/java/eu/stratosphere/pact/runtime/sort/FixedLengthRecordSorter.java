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

import eu.stratosphere.nephele.services.iomanager.ChannelWriterOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.io.RandomAccessInputView;
import eu.stratosphere.pact.runtime.io.RandomAccessOutputView;

/**
 * 
 */
public final class FixedLengthRecordSorter<T> implements IndexedSortable
{
	private static final int DEFAULT_MAX_NORMALIZED_KEY_LEN = 8;
	
	private static final int MIN_REQUIRED_BUFFERS = 3;

	// ------------------------------------------------------------------------
	//                               Members
	// ------------------------------------------------------------------------

	private final byte[] swapBuffer;
	
	private final TypeSerializer<T> serializer;
	
	private final TypeComparator<T> comparator;
	
	private MemorySegment currentSortBufferSegment;
	
	private int currentSortBufferOffset;
	
	private final ArrayList<MemorySegment> freeMemory;
	
	private final ArrayList<MemorySegment> sortBuffer;
	
	private RandomAccessOutputView sortBufferOutputView;
	
	private RandomAccessInputView sortBufferInputView;
	
	private long sortBufferBytes;
	
	private int numRecords;
	
	private final int numKeyBytes;
	
	private final int recordSize;
	
	private final int recordsPerSegment;
	
	private final int lastEntryOffset;
	
	private final int segmentSize;
	
	private final int totalNumBuffers;
	
	private final boolean normalizedKeyFullyDetermines;
	
	private final boolean useNormKeyUninverted;
	
	
	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------

	public FixedLengthRecordSorter(TypeSerializer<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory) {
		this(serializer, comparator, memory, DEFAULT_MAX_NORMALIZED_KEY_LEN);
	}
	
	public FixedLengthRecordSorter(TypeSerializer<T> serializer, TypeComparator<T> comparator, 
			List<MemorySegment> memory, int maxNormalizedKeyBytes)
	{
		if (serializer == null || comparator == null || memory == null)
			throw new NullPointerException();
		if (maxNormalizedKeyBytes < 0)
			throw new IllegalArgumentException("Maximal number of normalized key bytes must not be negative.");
		
		this.serializer = serializer;
		this.comparator = comparator;
		this.useNormKeyUninverted = !comparator.invertNormalizedKey();
		
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
		this.sortBuffer = new ArrayList<MemorySegment>(16);
		// set to initial state
		this.currentSortBufferSegment = nextMemorySegment();
		this.sortBuffer.add(this.currentSortBufferSegment);
		this.sortBufferOutputView = new RandomAccessOutputView(this.sortBuffer.toArray(new MemorySegment[this.sortBuffer.size()]), this.segmentSize);
		this.sortBufferInputView = new RandomAccessInputView(this.sortBuffer, this.segmentSize);
		
		// set up normalized key characteristics
		if (this.comparator.supportsNormalizedKey()) {
			this.numKeyBytes = Math.min(this.comparator.getNormalizeKeyLen(), maxNormalizedKeyBytes);
			this.normalizedKeyFullyDetermines = !this.comparator.isNormalizedKeyPrefixOnly(this.numKeyBytes);
		}
		else {
			this.numKeyBytes = 0;
			this.normalizedKeyFullyDetermines = false;
		}
		
		// compute the entry size and limits
		this.recordSize = serializer.getLength();
		if (this.recordSize == -1) {
			throw new IllegalArgumentException("We only accept fixed length records.");
		}
		this.recordsPerSegment = segmentSize / this.recordSize;
		this.lastEntryOffset = (this.recordsPerSegment - 1) * this.recordSize;
		this.swapBuffer = new byte[this.recordSize];
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
		this.currentSortBufferOffset = 0;
		this.sortBufferBytes = 0;
		
		// return all memory
		this.freeMemory.addAll(this.sortBuffer);
		this.sortBuffer.clear();
		
		// grab first buffers
		this.currentSortBufferSegment = nextMemorySegment();
		this.sortBuffer.add(this.currentSortBufferSegment);
		this.sortBufferOutputView = new RandomAccessOutputView(this.sortBuffer.toArray(new MemorySegment[this.sortBuffer.size()]), this.segmentSize);
		this.sortBufferInputView = new RandomAccessInputView(this.sortBuffer, this.segmentSize);
	}

	/**
	 * Checks whether the buffer is empty.
	 * 
	 * @return True, if no record is contained, false otherwise.
	 */
	public boolean isEmpty() {
		return this.numRecords == 0;
	}
	
	/**
	 * Collects all memory segments from this sorter.
	 * 
	 * @return All memory segments from this sorter.
	 */
	public List<MemorySegment> dispose() {
		this.freeMemory.addAll(this.sortBuffer);
		
		this.sortBuffer.clear();
		
		return this.freeMemory;
	}
	
	/**
	 * Gets the total capacity of this sorter, in bytes.
	 * 
	 * @return The sorter's total capacity.
	 */
	public long getCapacity() {
		return ((long) this.totalNumBuffers) * this.segmentSize;
	}
	
	/**
	 * Gets the number of bytes currently occupied in this sorter.
	 * 
	 * @return The number of bytes occupied.
	 */
	public long getOccupancy() {
		return this.sortBufferBytes;
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
	public void getRecord(T target, int logicalPosition) throws IOException {
		getRecordFromBuffer(target, logicalPosition * this.recordSize);
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
		if (this.currentSortBufferOffset > this.lastEntryOffset) {
			if (memoryAvailable()) {
				this.currentSortBufferSegment = nextMemorySegment();
				this.sortBuffer.add(this.currentSortBufferSegment);
				this.sortBufferOutputView = new RandomAccessOutputView(this.sortBuffer.toArray(new MemorySegment[this.sortBuffer.size()]), this.segmentSize);
				this.sortBufferOutputView.setWritePosition((sortBuffer.size()-1)*this.segmentSize);
				this.currentSortBufferOffset = 0;
				this.sortBufferBytes += this.segmentSize;
			}
			else {
				return false;
			}
		}
		
		// serialize the record into the data buffers
		try {
			this.serializer.serialize(record, this.sortBufferOutputView);
			this.numRecords++;
			// add the normalized key, overwriting the already written proper key
			this.comparator.putNormalizedKey(record, this.currentSortBufferSegment.getBackingArray(), this.currentSortBufferSegment.translateOffset(this.currentSortBufferOffset), this.numKeyBytes);
			
			this.currentSortBufferOffset += this.recordSize;
		
			return true;
		} catch (EOFException eofex) {
			
			return false;
		}
	}
	
	// ------------------------------------------------------------------------
	//                           Access Utilities
	// ------------------------------------------------------------------------
	private final void getRecordFromBuffer(T target, long pointer) throws IOException
	{
		this.sortBufferInputView.setReadPosition(pointer);
		this.serializer.deserialize(target, this.sortBufferInputView);
		this.sortBufferInputView.setReadPosition(pointer);
		this.comparator.readFromNormalizedKey(target,
	    		this.sortBufferInputView.getCurrentSegment().getBackingArray(),
				this.sortBufferInputView.getCurrentPositionInSegment(), this.numKeyBytes);
	}
	
	private final boolean memoryAvailable() {
		return !this.freeMemory.isEmpty();
	}
	
	private final MemorySegment nextMemorySegment() {
		return this.freeMemory.remove(this.freeMemory.size() - 1);
	}

	// -------------------------------------------------------------------------
	// Sorting
	// -------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.IndexedSortable#compare(int, int)
	 */
	public int compare(int i, int j)
	{
		final int bufferNumI = i / this.recordsPerSegment;
		final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;
		
		final int bufferNumJ = j / this.recordsPerSegment;
		final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;
		
		final MemorySegment segI = this.sortBuffer.get(bufferNumI);
		final MemorySegment segJ = this.sortBuffer.get(bufferNumJ);
		final byte[] bI = segI.getBackingArray();
		final byte[] bJ = segJ.getBackingArray();
		
		int val = 0;
		for (int pos = 0, posI = segI.translateOffset(segmentOffsetI), posJ = segJ.translateOffset(segmentOffsetJ);
			pos < this.numKeyBytes && (val = (bI[posI] & 0xff) - (bJ[posJ] & 0xff)) == 0; pos++, posI++, posJ++);
		
		if (val != 0 || this.normalizedKeyFullyDetermines) {
			return this.useNormKeyUninverted ? val : -val;
		}
		
		throw new IllegalStateException("We do not support directly comparing records here.");
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.sort.IndexedSortable#swap(int, int)
	 */
	@Override
	public void swap(int i, int j)
	{
		final int bufferNumI = i / this.recordsPerSegment;
		final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;
		
		final int bufferNumJ = j / this.recordsPerSegment;
		final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;
		
		final MemorySegment segI = this.sortBuffer.get(bufferNumI);
		final MemorySegment segJ = this.sortBuffer.get(bufferNumJ);
		
		segI.get(segmentOffsetI, this.swapBuffer, 0, this.recordSize);
		System.arraycopy(segJ.getBackingArray(), segJ.translateOffset(segmentOffsetJ), segI.getBackingArray(), segI.translateOffset(segmentOffsetI), this.recordSize);
		segJ.put(segmentOffsetJ, this.swapBuffer, 0, this.recordSize);
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
			
			private int currentSegmentIndex = 0;
			private int currentOffset = 0;

			@Override
			public boolean next(T target)
			{
				if (this.current < this.size) {
					this.current++;
					if ((this.currentOffset % segmentSize) > lastEntryOffset) {
						++this.currentSegmentIndex;
						this.currentOffset = this.currentSegmentIndex * segmentSize;
					}
					
					int pointer = this.currentOffset;
					this.currentOffset += recordSize;
					
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
//		int recordsLeft = this.numRecords;
//		int currentMemSeg = 0;
//		while (recordsLeft > 0)
//		{
//			final MemorySegment currentIndexSegment = this.sortBuffer.get(currentMemSeg++);
//			int offset = 0;
//			// check whether we have a full or partially full segment
//			if (recordsLeft >= this.recordsPerSegment) {
//				// full segment
//				for (;offset <= this.lastEntryOffset; offset += this.recordSize) {
//					final long pointer = currentIndexSegment.getLong(offset);
//					this.recordBuffer.setReadPosition(pointer);
//					this.serializer.copy(this.recordBuffer, output);
//					
//				}
//				recordsLeft -= this.recordsPerSegment;
//			} else {
//				// partially filled segment
//				for (; recordsLeft > 0; recordsLeft--, offset += this.recordSize)
//				{
//					final long pointer = currentIndexSegment.getLong(offset);
//					this.recordBuffer.setReadPosition(pointer);
//					this.serializer.copy(this.recordBuffer, output);
//				}
//			}
//		}
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
//		int currentMemSeg = start / this.recordsPerSegment;
//		int offset = (start % this.recordsPerSegment) * this.recordSize;
//		
//		while (num > 0)
//		{
//			final MemorySegment currentIndexSegment = this.sortBuffer.get(currentMemSeg++);
//			// check whether we have a full or partially full segment
//			if (num >= this.recordsPerSegment && offset == 0) {
//				// full segment
//				for (;offset <= this.lastEntryOffset; offset += this.recordSize) {
//					final long pointer = currentIndexSegment.getLong(offset);
//					this.recordBuffer.setReadPosition(pointer);
//					this.serializer.copy(this.recordBuffer, output);
//				}
//				num -= this.recordsPerSegment;
//			} else {
//				// partially filled segment
//				for (; num > 0 && offset <= this.lastEntryOffset; num--, offset += this.recordSize)
//				{
//					final long pointer = currentIndexSegment.getLong(offset);
//					this.recordBuffer.setReadPosition(pointer);
//					this.serializer.copy(this.recordBuffer, output);
//				}
//			}
//			offset = 0;
//		}
	}
}
