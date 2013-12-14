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

import eu.stratosphere.api.typeutils.TypeComparator;
import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.nephele.services.iomanager.ChannelWriterOutputView;
import eu.stratosphere.nephele.services.memorymanager.AbstractPagedInputView;
import eu.stratosphere.nephele.services.memorymanager.AbstractPagedOutputView;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * 
 */
public final class FixedLengthRecordSorter<T> implements InMemorySorter<T> {
	
	private static final int MIN_REQUIRED_BUFFERS = 3;

	// ------------------------------------------------------------------------
	//                               Members
	// ------------------------------------------------------------------------

	private final byte[] swapBuffer;
	
	private final TypeSerializer<T> serializer;
	
	private final TypeComparator<T> comparator;
	
	private final SingleSegmentOutputView outView;
	
	private final SingleSegmentInputView inView;
	
	private MemorySegment currentSortBufferSegment;
	
	private int currentSortBufferOffset;
	
	private final ArrayList<MemorySegment> freeMemory;
	
	private final ArrayList<MemorySegment> sortBuffer;
	
	private long sortBufferBytes;
	
	private int numRecords;
	
	private final int numKeyBytes;
	
	private final int recordSize;
	
	private final int recordsPerSegment;
	
	private final int lastEntryOffset;
	
	private final int segmentSize;
	
	private final int totalNumBuffers;
	
	private final boolean useNormKeyUninverted;
	
	private final T recordInstance;
	
	
	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------
	
	public FixedLengthRecordSorter(TypeSerializer<T> serializer, TypeComparator<T> comparator, 
			List<MemorySegment> memory)
	{
		if (serializer == null || comparator == null || memory == null)
			throw new NullPointerException();
		
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
		this.recordSize = serializer.getLength();
		this.numKeyBytes = this.comparator.getNormalizeKeyLen();
		
		// check that the serializer and comparator allow our operations
		if (this.recordSize <= 0) {
			throw new IllegalArgumentException("This sorter works only for fixed-length data types.");
		} else if (this.recordSize > this.segmentSize) {
			throw new IllegalArgumentException("This sorter works only for record lengths below the memory segment size.");
		} else if (!comparator.supportsSerializationWithKeyNormalization()) {
			throw new IllegalArgumentException("This sorter requires a comparator that supports serialization with key normalization.");
		}
		
		// compute the entry size and limits
		this.recordsPerSegment = segmentSize / this.recordSize;
		this.lastEntryOffset = (this.recordsPerSegment - 1) * this.recordSize;
		this.swapBuffer = new byte[this.recordSize];
		
		if (memory instanceof ArrayList<?>) {
			this.freeMemory = (ArrayList<MemorySegment>) memory;
		}
		else {
			this.freeMemory = new ArrayList<MemorySegment>(memory.size());
			this.freeMemory.addAll(memory);
		}
		
		// create the buffer collections
		this.sortBuffer = new ArrayList<MemorySegment>(16);
		this.outView = new SingleSegmentOutputView(this.segmentSize);
		this.inView = new SingleSegmentInputView(this.lastEntryOffset + this.recordSize);
		this.currentSortBufferSegment = nextMemorySegment();
		this.sortBuffer.add(this.currentSortBufferSegment);
		this.outView.set(this.currentSortBufferSegment);
		
		this.recordInstance = this.serializer.createInstance();
	}

	// -------------------------------------------------------------------------
	// Memory Segment
	// -------------------------------------------------------------------------

	/**
	 * Resets the sort buffer back to the state where it is empty. All contained data is discarded.
	 */
	@Override
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
		this.outView.set(this.currentSortBufferSegment);
	}

	/**
	 * Checks whether the buffer is empty.
	 * 
	 * @return True, if no record is contained, false otherwise.
	 */
	@Override
	public boolean isEmpty() {
		return this.numRecords == 0;
	}
	
	/**
	 * Collects all memory segments from this sorter.
	 * 
	 * @return All memory segments from this sorter.
	 */
	@Override
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
	@Override
	public long getCapacity() {
		return ((long) this.totalNumBuffers) * this.segmentSize;
	}
	
	/**
	 * Gets the number of bytes currently occupied in this sorter.
	 * 
	 * @return The number of bytes occupied.
	 */
	@Override
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
	@Override
	public void getRecord(T target, int logicalPosition) throws IOException {
		final int buffer = logicalPosition / this.recordsPerSegment;
		final int inBuffer = (logicalPosition % this.recordsPerSegment) * this.recordSize;
		this.inView.set(this.sortBuffer.get(buffer), inBuffer);
		this.comparator.readWithKeyDenormalization(target, this.inView);
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
		// check whether we need a new memory segment for the sort index
		if (this.currentSortBufferOffset > this.lastEntryOffset) {
			if (memoryAvailable()) {
				this.currentSortBufferSegment = nextMemorySegment();
				this.sortBuffer.add(this.currentSortBufferSegment);
				this.outView.set(this.currentSortBufferSegment);
				this.currentSortBufferOffset = 0;
				this.sortBufferBytes += this.segmentSize;
			}
			else {
				return false;
			}
		}
		
		// serialize the record into the data buffers
		try {
			this.comparator.writeWithKeyNormalization(record, this.outView);
			this.numRecords++;
			this.currentSortBufferOffset += this.recordSize;
			return true;
		} catch (EOFException eofex) {
			throw new IOException("Error: Serialization consumes more bytes than announced by the serializer.");
		}
	}
	
	// ------------------------------------------------------------------------
	//                           Access Utilities
	// ------------------------------------------------------------------------
	
	private final boolean memoryAvailable() {
		return !this.freeMemory.isEmpty();
	}
	
	private final MemorySegment nextMemorySegment() {
		return this.freeMemory.remove(this.freeMemory.size() - 1);
	}

	// -------------------------------------------------------------------------
	// Sorting
	// -------------------------------------------------------------------------

	@Override
	public int compare(int i, int j) {
		final int bufferNumI = i / this.recordsPerSegment;
		final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;
		
		final int bufferNumJ = j / this.recordsPerSegment;
		final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;
		
		final MemorySegment segI = this.sortBuffer.get(bufferNumI);
		final MemorySegment segJ = this.sortBuffer.get(bufferNumJ);
		
		int val = MemorySegment.compare(segI, segJ, segmentOffsetI, segmentOffsetJ, this.numKeyBytes);
		return this.useNormKeyUninverted ? val : -val;
	}

	@Override
	public void swap(int i, int j) {
		final int bufferNumI = i / this.recordsPerSegment;
		final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;
		
		final int bufferNumJ = j / this.recordsPerSegment;
		final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;
		
		final MemorySegment segI = this.sortBuffer.get(bufferNumI);
		final MemorySegment segJ = this.sortBuffer.get(bufferNumJ);
		
		MemorySegment.swapBytes(segI, segJ, this.swapBuffer, segmentOffsetI, segmentOffsetJ, this.recordSize);
	}

	@Override
	public int size() {
		return this.numRecords;
	}

	// -------------------------------------------------------------------------
	
	/**
	 * Gets an iterator over all records in this buffer in their logical order.
	 * 
	 * @return An iterator returning the records in their logical order.
	 */
	@Override
	public final MutableObjectIterator<T> getIterator() {
		final SingleSegmentInputView startIn = new SingleSegmentInputView(this.recordsPerSegment * this.recordSize);
		startIn.set(this.sortBuffer.get(0), 0);
		
		return new MutableObjectIterator<T>() {
			
			private final SingleSegmentInputView in = startIn;
			private final TypeComparator<T> comp = comparator;
			
			private final int numTotal = size();
			private final int numPerSegment = recordsPerSegment;
			
			private int currentTotal = 0;
			private int currentInSegment = 0;
			private int currentSegmentIndex = 0;

			@Override
			public boolean next(T target) {
				if (this.currentTotal < this.numTotal) {
					
					if (this.currentInSegment >= this.numPerSegment) {
						this.currentInSegment = 0;
						this.currentSegmentIndex++;
						this.in.set(sortBuffer.get(this.currentSegmentIndex), 0);
					}
					
					this.currentTotal++;
					this.currentInSegment++;
					
					try {
						this.comp.readWithKeyDenormalization(target, this.in);
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
	@Override
	public void writeToOutput(final ChannelWriterOutputView output) throws IOException {
		final TypeComparator<T> comparator = this.comparator;
		final TypeSerializer<T> serializer = this.serializer;
		final T record = this.recordInstance;
		
		final SingleSegmentInputView inView = this.inView;
		
		final int recordsPerSegment = this.recordsPerSegment;
		int recordsLeft = this.numRecords;
		int currentMemSeg = 0;
		
		while (recordsLeft > 0) {
			final MemorySegment currentIndexSegment = this.sortBuffer.get(currentMemSeg++);
			inView.set(currentIndexSegment, 0);
			
			// check whether we have a full or partially full segment
			if (recordsLeft >= recordsPerSegment) {
				// full segment
				for (int numInMemSeg = 0; numInMemSeg < recordsPerSegment; numInMemSeg++) {
					comparator.readWithKeyDenormalization(record, inView);
					serializer.serialize(record, output);
				}
				recordsLeft -= recordsPerSegment;
			} else {
				// partially filled segment
				for (; recordsLeft > 0; recordsLeft--) {
					comparator.readWithKeyDenormalization(record, inView);
					serializer.serialize(record, output);
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
	@Override
	public void writeToOutput(final ChannelWriterOutputView output, final int start, int num) throws IOException {
		final TypeComparator<T> comparator = this.comparator;
		final TypeSerializer<T> serializer = this.serializer;
		final T record = this.recordInstance;
		
		final SingleSegmentInputView inView = this.inView;
		
		final int recordsPerSegment = this.recordsPerSegment;
		int currentMemSeg = start / recordsPerSegment;
		int offset = (start % recordsPerSegment) * this.recordSize;
		
		while (num > 0) {
			final MemorySegment currentIndexSegment = this.sortBuffer.get(currentMemSeg++);
			inView.set(currentIndexSegment, offset);
			
			// check whether we have a full or partially full segment
			if (num >= recordsPerSegment && offset == 0) {
				// full segment
				for (int numInMemSeg = 0; numInMemSeg < recordsPerSegment; numInMemSeg++) {
					comparator.readWithKeyDenormalization(record, inView);
					serializer.serialize(record, output);
				}
				num -= recordsPerSegment;
			} else {
				// partially filled segment
				for (; num > 0; num--) {
					comparator.readWithKeyDenormalization(record, inView);
					serializer.serialize(record, output);
				}
			}
		}
	}
	
	private static final class SingleSegmentOutputView extends AbstractPagedOutputView {
		
		SingleSegmentOutputView(int segmentSize) {
			super(segmentSize, 0);
		}
		
		void set(MemorySegment segment) {
			seekOutput(segment, 0);
		}
		
		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException {
			throw new EOFException();
		}
	}
	
	private static final class SingleSegmentInputView extends AbstractPagedInputView {
		
		private final int limit;
		
		SingleSegmentInputView(int limit) {
			super(0);
			this.limit = limit;
		}
		
		protected void set(MemorySegment segment, int offset) {
			seekInput(segment, offset, this.limit);
		}
		
		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
			throw new EOFException();
		}

		@Override
		protected int getLimitForSegment(MemorySegment segment) {
			return this.limit;
		}
	}
}
