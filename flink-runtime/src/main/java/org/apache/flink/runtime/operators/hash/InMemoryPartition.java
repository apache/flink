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


package org.apache.flink.runtime.operators.hash;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.runtime.memory.ListMemorySegmentSource;

/**
 * In-memory partition with overflow buckets for {@link CompactingHashTable}
 * 
 * @param <T> record type
 */
public class InMemoryPartition<T> {
	
	// --------------------------------- Table Structure Auxiliaries ------------------------------------
	
	protected MemorySegment[] overflowSegments;	// segments in which overflow buckets from the table structure are stored
	
	protected int numOverflowSegments;			// the number of actual segments in the overflowSegments array
	
	protected int nextOverflowBucket;				// the next free bucket in the current overflow segment

	// -------------------------------------  Type Accessors --------------------------------------------
	
	private final TypeSerializer<T> serializer;
	
	// -------------------------------------- Record Buffers --------------------------------------------
	
	private final ArrayList<MemorySegment> partitionPages;
	
	private final ListMemorySegmentSource availableMemory;
	
	private WriteView writeView;
	
	private ReadView readView;
	
	private long recordCounter;				// number of records in this partition including garbage
	
	// ----------------------------------------- General ------------------------------------------------
	
	private int partitionNumber;					// the number of the partition
	
	private boolean compacted;						// overwritten records since allocation or last full compaction
	
	private int pageSize;							// segment size in bytes
	
	private int pageSizeInBits;
	
	// --------------------------------------------------------------------------------------------------
	
	
	
	/**
	 * Creates a new partition, in memory, with one buffer. 
	 * 
	 * @param serializer Serializer for T.
	 * @param partitionNumber The number of the partition.
	 * @param memSource memory pool
	 * @param pageSize segment size in bytes
	 * @param pageSizeInBits
	 */
	public InMemoryPartition(TypeSerializer<T> serializer, int partitionNumber,
			ListMemorySegmentSource memSource, int pageSize, int pageSizeInBits)
	{
		this.overflowSegments = new MemorySegment[2];
		this.numOverflowSegments = 0;
		this.nextOverflowBucket = 0;
		
		this.serializer = serializer;
		this.partitionPages = new ArrayList<MemorySegment>(64);
		this.availableMemory = memSource;
		
		this.partitionNumber = partitionNumber;
		
		// add the first segment
		this.partitionPages.add(memSource.nextSegment());
		// empty partitions have no garbage
		this.compacted = true;
		
		this.pageSize = pageSize;
		
		this.pageSizeInBits = pageSizeInBits;
		
		this.writeView = new WriteView(this.partitionPages, memSource, pageSize, pageSizeInBits);
		this.readView = new ReadView(this.partitionPages, pageSize, pageSizeInBits);
	}
	
	// --------------------------------------------------------------------------------------------------
	
	/**
	 * Gets the partition number of this partition.
	 * 
	 * @return This partition's number.
	 */
	public int getPartitionNumber() {
		return this.partitionNumber;
	}
	
	/**
	 * overwrites partition number and should only be used on compaction partition
	 * @param number new partition
	 */
	public void setPartitionNumber(int number) {
		this.partitionNumber = number;
	}
	
	/**
	 * 
	 * @return number of segments owned by partition
	 */
	public int getBlockCount() {
		return this.partitionPages.size();
	}
	
	/**
	 * number of records in partition including garbage
	 * 
	 * @return number record count
	 */
	public long getRecordCount() {
		return this.recordCounter;
	}
	
	/**
	 * sets record counter to zero and should only be used on compaction partition
	 */
	public void resetRecordCounter() {
		this.recordCounter = 0L;
	}
	
	/**
	 * resets read and write views and should only be used on compaction partition
	 */
	public void resetRWViews() {
		this.writeView.resetTo(0L);
		this.readView.setReadPosition(0L);
	}
	
	public void pushDownPages() {
		this.writeView = new WriteView(this.partitionPages, availableMemory, pageSize, pageSizeInBits);
		this.readView = new ReadView(this.partitionPages, pageSize, pageSizeInBits);
	}
	
	/**
	 * resets overflow bucket counters and returns freed memory and should only be used for resizing
	 * 
	 * @return freed memory segments
	 */
	public ArrayList<MemorySegment> resetOverflowBuckets() {
		this.numOverflowSegments = 0;
		this.nextOverflowBucket = 0;
		
		ArrayList<MemorySegment> result = new ArrayList<MemorySegment>(this.overflowSegments.length);
		for(int i = 0; i < this.overflowSegments.length; i++) {
			if(this.overflowSegments[i] != null) {
				result.add(this.overflowSegments[i]);
			}
		}
		this.overflowSegments = new MemorySegment[2];
		return result;
	}
	
	/**
	 * @return true if garbage exists in partition
	 */
	public boolean isCompacted() {
		return this.compacted;
	}
	
	/**
	 * sets compaction status (should only be set <code>true</code> directly after compaction and <code>false</code> when garbage was created)
	 * 
	 * @param compacted compaction status
	 */
	public void setIsCompacted(boolean compacted) {
		this.compacted = compacted;
	}
	
	// --------------------------------------------------------------------------------------------------
	
	/**
	 * Inserts the given object into the current buffer. This method returns a pointer that
	 * can be used to address the written record in this partition.
	 * 
	 * @param record The object to be written to the partition.
	 * @return A pointer to the object in the partition.
	 * @throws IOException Thrown when the write failed.
	 */
	public final long appendRecord(T record) throws IOException {
		long pointer = this.writeView.getCurrentPointer();
		try {
			this.serializer.serialize(record, this.writeView);
			this.recordCounter++;
			return pointer;
		} catch (EOFException e) {
			// we ran out of pages. 
			// first, reset the pages and then we need to trigger a compaction
			//int oldCurrentBuffer = 
			this.writeView.resetTo(pointer);
			//for (int bufNum = this.partitionPages.size() - 1; bufNum > oldCurrentBuffer; bufNum--) {
			//	this.availableMemory.addMemorySegment(this.partitionPages.remove(bufNum));
			//}
			throw e;
		}
	}
	
	public T readRecordAt(long pointer, T reuse) throws IOException {
		this.readView.setReadPosition(pointer);
		return this.serializer.deserialize(reuse, this.readView);
	}

	public T readRecordAt(long pointer) throws IOException {
		this.readView.setReadPosition(pointer);
		return this.serializer.deserialize(this.readView);
	}
	
	/**
	 * UNSAFE!! overwrites record
	 * causes inconsistency or data loss for overwriting everything but records of the exact same size
	 * 
	 * @param pointer pointer to start of record
	 * @param record record to overwrite old one with
	 * @throws IOException
	 * @deprecated Don't use this, overwrites record and causes inconsistency or data loss for
	 * overwriting everything but records of the exact same size
	 */
	@Deprecated
	public void overwriteRecordAt(long pointer, T record) throws IOException {
		long tmpPointer = this.writeView.getCurrentPointer();
		this.writeView.resetTo(pointer);
		this.serializer.serialize(record, this.writeView);
		this.writeView.resetTo(tmpPointer);
	}
	
	/**
	 * releases all of the partition's segments (pages and overflow buckets)
	 * 
	 * @param target memory pool to release segments to
	 */
	public void clearAllMemory(List<MemorySegment> target) {
		// return the overflow segments
		if (this.overflowSegments != null) {
			for (int k = 0; k < this.numOverflowSegments; k++) {
				target.add(this.overflowSegments[k]);
			}
		}	
		// return the partition buffers
		target.addAll(this.partitionPages);
		this.partitionPages.clear();
	}
	
	/**
	 * attempts to allocate specified number of segments and should only be used by compaction partition
	 * fails silently if not enough segments are available since next compaction could still succeed
	 * 
	 * @param numberOfSegments allocation count
	 */
	public void allocateSegments(int numberOfSegments) {
		while (getBlockCount() < numberOfSegments) {
			MemorySegment next = this.availableMemory.nextSegment();
			if (next != null) {
				this.partitionPages.add(next);
			} else {
				return;
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("Partition %d - %d records, %d partition blocks, %d bucket overflow blocks", getPartitionNumber(), getRecordCount(), getBlockCount(), this.numOverflowSegments);
	}
	
	// ============================================================================================
	
	private static final class WriteView extends AbstractPagedOutputView {
		
		private final ArrayList<MemorySegment> pages;
		
		private final MemorySegmentSource memSource;
		
		private final int sizeBits;
		
		private final int sizeMask;
		
		private int currentPageNumber;
		
		private int segmentNumberOffset;
		
		
		private WriteView(ArrayList<MemorySegment> pages, MemorySegmentSource memSource,
				int pageSize, int pageSizeBits)
		{
			super(pages.get(0), pageSize, 0);
			
			this.pages = pages;
			this.memSource = memSource;
			this.sizeBits = pageSizeBits;
			this.sizeMask = pageSize - 1;
			this.segmentNumberOffset = 0;
		}
		

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int bytesUsed) throws IOException {
			MemorySegment next = this.memSource.nextSegment();
			if(next == null) {
				throw new EOFException();
			}
			this.pages.add(next);
			
			this.currentPageNumber++;
			return next;
		}
		
		private long getCurrentPointer() {
			return (((long) this.currentPageNumber) << this.sizeBits) + getCurrentPositionInSegment();
		}
		
		private int resetTo(long pointer) {
			final int pageNum  = (int) (pointer >>> this.sizeBits);
			final int offset = (int) (pointer & this.sizeMask);
			
			this.currentPageNumber = pageNum;
			
			int posInArray = pageNum - this.segmentNumberOffset;
			seekOutput(this.pages.get(posInArray), offset);
			
			return posInArray;
		}
		
		@SuppressWarnings("unused")
		public void setSegmentNumberOffset(int offset) {
			this.segmentNumberOffset = offset;
		}
	}
	
	
	private static final class ReadView extends AbstractPagedInputView implements SeekableDataInputView {

		private final ArrayList<MemorySegment> segments;

		private final int segmentSizeBits;
		
		private final int segmentSizeMask;
		
		private int currentSegmentIndex;
		
		private int segmentNumberOffset;
		
		
		public ReadView(ArrayList<MemorySegment> segments, int segmentSize, int segmentSizeBits) {
			super(segments.get(0), segmentSize, 0);
			
			if ((segmentSize & (segmentSize - 1)) != 0) {
				throw new IllegalArgumentException("Segment size must be a power of 2!");
			}
			
			this.segments = segments;
			this.segmentSizeBits = segmentSizeBits;
			this.segmentSizeMask = segmentSize - 1;
			this.segmentNumberOffset = 0;
		}

		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
			if (++this.currentSegmentIndex < this.segments.size()) {
				return this.segments.get(this.currentSegmentIndex);
			} else {
				throw new EOFException();
			}
		}

		@Override
		protected int getLimitForSegment(MemorySegment segment) {
			return this.segmentSizeMask + 1;
		}
		
		@Override
		public void setReadPosition(long position) {
			final int bufferNum = ((int) (position >>> this.segmentSizeBits)) - this.segmentNumberOffset;
			final int offset = (int) (position & this.segmentSizeMask);
			
			this.currentSegmentIndex = bufferNum;
			seekInput(this.segments.get(bufferNum), offset, this.segmentSizeMask + 1);
		}
		
		@SuppressWarnings("unused")
		public void setSegmentNumberOffset(int offset) {
			this.segmentNumberOffset = offset;
		}
	}

}
