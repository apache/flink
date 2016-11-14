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
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.core.memory.SeekableDataOutputView;
import org.apache.flink.runtime.io.disk.RandomAccessOutputView;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.MutableObjectIterator;

/**
 * 
 * @param <BT> The type of the build side records.
 * @param <PT> The type of the probe side records.
 */
public class HashPartition<BT, PT> extends AbstractPagedInputView implements SeekableDataInputView {
	
	// --------------------------------- Table Structure Auxiliaries ------------------------------------
	
	protected MemorySegment[] overflowSegments;	// segments in which overflow buckets from the table structure are stored
	
	protected int numOverflowSegments;			// the number of actual segments in the overflowSegments array
	
	protected int nextOverflowBucket;				// the next free bucket in the current overflow segment

	// -------------------------------------  Type Accessors --------------------------------------------
	
	private final TypeSerializer<BT> buildSideSerializer;
	
	private final TypeSerializer<PT> probeSideSerializer;
	
	// -------------------------------------- Record Buffers --------------------------------------------
	
	protected MemorySegment[] partitionBuffers;
	
	private int currentBufferNum;
	
	private int finalBufferLimit;
	
	private BuildSideBuffer buildSideWriteBuffer;
	
	protected ChannelWriterOutputView probeSideBuffer;
	
	private RandomAccessOutputView overwriteBuffer;
	
	private long buildSideRecordCounter;				// number of build-side records in this partition
	
	protected long probeSideRecordCounter;				// number of probe-side records in this partition 
	
	// ----------------------------------------- General ------------------------------------------------
	
	private final int segmentSizeBits;					// the number of bits in the mem segment size;
	
	private final int memorySegmentSize;				// the size of the memory segments being used
	
	private final int partitionNumber;					// the number of the partition
	
	protected int recursionLevel;									// the recursion level on which this partition lives
	
	// ------------------------------------------ Spilling ----------------------------------------------
	
	private BlockChannelWriter<MemorySegment> buildSideChannel;		// the channel writer for the build side, if partition is spilled
	
	protected BlockChannelWriter<MemorySegment> probeSideChannel;		// the channel writer from the probe side, if partition is spilled
	
	// ------------------------------------------ Restoring ----------------------------------------------
	
	protected boolean furtherPartitioning = false;
	
	protected void setFurtherPatitioning(boolean v) {
		furtherPartitioning = v;
	}
	
	// --------------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new partition, initially in memory, with one buffer for the build side. The partition is
	 * initialized to expect record insertions for the build side.
	 * 
	 * @param partitionNumber The number of the partition.
	 * @param recursionLevel The recursion level - zero for partitions from the initial build, <i>n + 1</i> for
	 *                       partitions that are created from spilled partition with recursion level <i>n</i>. 
	 * @param initialBuffer The initial buffer for this partition.
	 */
	HashPartition(TypeSerializer<BT> buildSideAccessors, TypeSerializer<PT> probeSideAccessors,
			int partitionNumber, int recursionLevel, MemorySegment initialBuffer, MemorySegmentSource memSource,
			int segmentSize)
	{
		super(0);
		
		this.buildSideSerializer = buildSideAccessors;
		this.probeSideSerializer = probeSideAccessors;
		this.partitionNumber = partitionNumber;
		this.recursionLevel = recursionLevel;
		
		this.memorySegmentSize = segmentSize;
		this.segmentSizeBits = MathUtils.log2strict(segmentSize);
		
		this.overflowSegments = new MemorySegment[2];
		this.numOverflowSegments = 0;
		this.nextOverflowBucket = 0;
		
		this.buildSideWriteBuffer = new BuildSideBuffer(initialBuffer, memSource);
	}
	
	/**
	 * Constructor creating a partition from a spilled partition file that could be read in one because it was
	 * known to completely fit into memory.
	 * 
	 * @param buildSideAccessors The data type accessors for the build side data-type.
	 * @param probeSideAccessors The data type accessors for the probe side data-type.
	 * @param partitionNumber The number of the partition.
	 * @param recursionLevel The recursion level of the partition.
	 * @param buffers The memory segments holding the records.
	 * @param buildSideRecordCounter The number of records in the buffers.
	 * @param segmentSize The size of the memory segments.
	 */
	HashPartition(TypeSerializer<BT> buildSideAccessors, TypeSerializer<PT> probeSideAccessors,
			int partitionNumber, int recursionLevel, List<MemorySegment> buffers,
			long buildSideRecordCounter, int segmentSize, int lastSegmentLimit)
	{
		super(0);
		
		this.buildSideSerializer = buildSideAccessors;
		this.probeSideSerializer = probeSideAccessors;
		this.partitionNumber = partitionNumber;
		this.recursionLevel = recursionLevel;
		
		this.memorySegmentSize = segmentSize;
		this.segmentSizeBits = MathUtils.log2strict(segmentSize);
		this.finalBufferLimit = lastSegmentLimit;
		
		this.partitionBuffers = (MemorySegment[]) buffers.toArray(new MemorySegment[buffers.size()]);
		this.buildSideRecordCounter = buildSideRecordCounter;
		
		this.overflowSegments = new MemorySegment[2];
		this.numOverflowSegments = 0;
		this.nextOverflowBucket = 0;
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
	 * Gets this partition's recursion level.
	 * 
	 * @return The partition's recursion level.
	 */
	public int getRecursionLevel() {
		return this.recursionLevel;
	}
	
	/**
	 * Checks whether this partition is in memory or spilled.
	 * 
	 * @return True, if the partition is in memory, false if it is spilled.
	 */
	public final boolean isInMemory() {
		return this.buildSideChannel == null;
	}

	/**
	 * Gets the number of memory segments used by this partition, which includes build side
	 * memory buffers and overflow memory segments.
	 * 
	 * @return The number of occupied memory segments.
	 */
	public int getNumOccupiedMemorySegments() {
		// either the number of memory segments, or one for spilling
		final int numPartitionBuffers = this.partitionBuffers != null ?
			this.partitionBuffers.length : this.buildSideWriteBuffer.getNumOccupiedMemorySegments();
		return numPartitionBuffers + numOverflowSegments;
	}
	
	
	public int getBuildSideBlockCount() {
		return this.partitionBuffers == null ? this.buildSideWriteBuffer.getBlockCount() : this.partitionBuffers.length;
	}
	
	public int getProbeSideBlockCount() {
		return this.probeSideBuffer == null ? -1 : this.probeSideBuffer.getBlockCount();
	}
	
	public long getBuildSideRecordCount() {
		return this.buildSideRecordCounter;
	}
	
	public long getProbeSideRecordCount() {
		return this.probeSideRecordCounter;
	}

	public BlockChannelWriter<MemorySegment> getBuildSideChannel() {
		return this.buildSideChannel;
	}
	
	
	public BlockChannelWriter<MemorySegment> getProbeSideChannel() {
		return this.probeSideChannel;
	}
	
	// --------------------------------------------------------------------------------------------------
	
	/**
	 * Inserts the given object into the current buffer. This method returns a pointer that
	 * can be used to address the written record in this partition, if it is in-memory. The returned
	 * pointers have no expressiveness in the case where the partition is spilled.
	 * 
	 * @param record The object to be written to the partition.
	 * @return A pointer to the object in the partition, or <code>-1</code>, if the partition is spilled.
	 * @throws IOException Thrown, when this is a spilled partition and the write failed.
	 */
	public final long insertIntoBuildBuffer(BT record) throws IOException {
		this.buildSideRecordCounter++;
		
		if (isInMemory()) {
			final long pointer = this.buildSideWriteBuffer.getCurrentPointer();
			this.buildSideSerializer.serialize(record, this.buildSideWriteBuffer);
			return isInMemory() ? pointer : -1;
		} else {
			this.buildSideSerializer.serialize(record, this.buildSideWriteBuffer);
			return -1;
		}
	}
	
	
	/**
	 * Inserts the given record into the probe side buffers. This method is only applicable when the
	 * partition was spilled while processing the build side.
	 * <p>
	 * If this method is invoked when the partition is still being built, it has undefined behavior.
	 *   
	 * @param record The record to be inserted into the probe side buffers.
	 * @throws IOException Thrown, if the buffer is full, needs to be spilled, and spilling causes an error.
	 */
	public final void insertIntoProbeBuffer(PT record) throws IOException {
		this.probeSideSerializer.serialize(record, this.probeSideBuffer);
		this.probeSideRecordCounter++;
	}
	
	/**
	 * Spills this partition to disk and sets it up such that it continues spilling records that are added to
	 * it. The spilling process must free at least one buffer, either in the partition's record buffers, or in
	 * the memory segments for overflow buckets.
	 * The partition immediately takes back one buffer to use it for further spilling.
	 * 
	 * @param target The list to which memory segments from overflow buckets are added.
	 * @param ioAccess The I/O manager to be used to create a writer to disk.
	 * @param targetChannel The id of the target channel for this partition.
	 * @return The number of buffers that were freed by spilling this partition.
	 * @throws IOException Thrown, if the writing failed.
	 */
	public int spillPartition(List<MemorySegment> target, IOManager ioAccess, FileIOChannel.ID targetChannel,
			LinkedBlockingQueue<MemorySegment> bufferReturnQueue)
	throws IOException
	{
		// sanity checks
		if (!isInMemory()) {
			throw new RuntimeException("Bug in Hybrid Hash Join: " +
					"Request to spill a partition that has already been spilled.");
		}
		if (getNumOccupiedMemorySegments() < 2) {
			throw new RuntimeException("Bug in Hybrid Hash Join: " +
				"Request to spill a partition with less than two buffers.");
		}
		
		// return the memory from the overflow segments
		for (int i = 0; i < this.numOverflowSegments; i++) {
			target.add(this.overflowSegments[i]);
		}
		this.overflowSegments = null;
		this.numOverflowSegments = 0;
		this.nextOverflowBucket = 0;
		
		// create the channel block writer and spill the current buffers
		// that keep the build side buffers current block, as it is most likely not full, yet
		// we return the number of blocks that become available
		this.buildSideChannel = ioAccess.createBlockChannelWriter(targetChannel, bufferReturnQueue);
		return this.buildSideWriteBuffer.spill(this.buildSideChannel);
	}
	
	public void finalizeBuildPhase(IOManager ioAccess, FileIOChannel.Enumerator probeChannelEnumerator,
			LinkedBlockingQueue<MemorySegment> bufferReturnQueue)
	throws IOException
	{
		this.finalBufferLimit = this.buildSideWriteBuffer.getCurrentPositionInSegment();
		this.partitionBuffers = this.buildSideWriteBuffer.close();
		
		if (!isInMemory()) {
			// close the channel. note that in the spilled case, the build-side-buffer will have sent off
			// the last segment and it will be returned to the write-behind-buffer queue.
			this.buildSideChannel.close();
			
			// create the channel for the probe side and claim one buffer for it
			this.probeSideChannel = ioAccess.createBlockChannelWriter(probeChannelEnumerator.next(), bufferReturnQueue);
			// creating the ChannelWriterOutputView without memory will cause it to draw one segment from the
			// write behind queue, which is the spare segment we had above.
			this.probeSideBuffer = new ChannelWriterOutputView(this.probeSideChannel, this.memorySegmentSize);
		}
	}
	
	/**
	 * @param keepUnprobedSpilledPartitions If true then partitions that were spilled but received no further probe
	 *                                      requests will be retained; used for build-side outer joins.
	 * @return The number of write-behind buffers reclaimable after this method call.
	 * 
	 * @throws IOException
	 */
	public int finalizeProbePhase(List<MemorySegment> freeMemory, List<HashPartition<BT, PT>> spilledPartitions,
			boolean keepUnprobedSpilledPartitions)
	throws IOException
	{
		if (isInMemory()) {
			// in this case, return all memory buffers
			
			// return the overflow segments
			for (int k = 0; k < this.numOverflowSegments; k++) {
				freeMemory.add(this.overflowSegments[k]);
			}
			this.overflowSegments = null;
			this.numOverflowSegments = 0;
			this.nextOverflowBucket = 0;
			// return the partition buffers
			for (MemorySegment partitionBuffer : this.partitionBuffers) {
				freeMemory.add(partitionBuffer);
			}
			this.partitionBuffers = null;
			return 0;
		}
		else if (this.probeSideRecordCounter == 0 && !keepUnprobedSpilledPartitions) {
			// partition is empty, no spilled buffers
			// return the memory buffer
			freeMemory.add(this.probeSideBuffer.getCurrentSegment());

			// delete the spill files
			this.probeSideChannel.close();
			this.buildSideChannel.deleteChannel();
			this.probeSideChannel.deleteChannel();
			return 0;
		}
		else {
			// flush the last probe side buffer and register this partition as pending
			this.probeSideBuffer.close();
			this.probeSideChannel.close();
			spilledPartitions.add(this);
			return 1;
		}
	}
	
	
	public void clearAllMemory(List<MemorySegment> target) {
		// return current buffers from build side and probe side
		if (this.buildSideWriteBuffer != null) {
			if (this.buildSideWriteBuffer.getCurrentSegment() != null) {
				target.add(this.buildSideWriteBuffer.getCurrentSegment());
			}
			target.addAll(this.buildSideWriteBuffer.targetList);
			this.buildSideWriteBuffer.targetList.clear();
			this.buildSideWriteBuffer = null;
		}
		if (this.probeSideBuffer != null && this.probeSideBuffer.getCurrentSegment() != null) {
			target.add(this.probeSideBuffer.getCurrentSegment());
			this.probeSideBuffer = null;
		}
		
		// return the overflow segments
		if (this.overflowSegments != null) {
			for (int k = 0; k < this.numOverflowSegments; k++) {
				target.add(this.overflowSegments[k]);
			}
		}
		
		// return the partition buffers
		if (this.partitionBuffers != null) {
			for (MemorySegment partitionBuffer : this.partitionBuffers) {
				target.add(partitionBuffer);
			}
			this.partitionBuffers = null;
		}
		
		// clear the channels
		try {
			if (this.buildSideChannel != null) {
				this.buildSideChannel.close();
				this.buildSideChannel.deleteChannel();
			}
			if (this.probeSideChannel != null) {
				this.probeSideChannel.close();
				this.probeSideChannel.deleteChannel();
			}
		}
		catch (IOException ioex) {
			throw new RuntimeException("Error deleting the partition files. Some temporary files might not be removed.");
		}
	}
	
	final PartitionIterator getPartitionIterator(TypeComparator<BT> comparator) throws IOException {
		return new PartitionIterator(comparator);
	}
	
	final int getLastSegmentLimit() {
		return this.finalBufferLimit;
	}
	
	final SeekableDataOutputView getWriteView() {
		if (this.overwriteBuffer == null) {
			this.overwriteBuffer = new RandomAccessOutputView(this.partitionBuffers, this.memorySegmentSize);
		}
		return this.overwriteBuffer;
	}
	
	// --------------------------------------------------------------------------------------------------
	//                   ReOpenableHashTable related methods
	// --------------------------------------------------------------------------------------------------
	
	public void prepareProbePhase(IOManager ioAccess, FileIOChannel.Enumerator probeChannelEnumerator,
			LinkedBlockingQueue<MemorySegment> bufferReturnQueue) throws IOException {
		if (isInMemory()) {
			return;
		}
		// ATTENTION: The following lines are duplicated code from finalizeBuildPhase
		this.probeSideChannel = ioAccess.createBlockChannelWriter(probeChannelEnumerator.next(), bufferReturnQueue);
		this.probeSideBuffer = new ChannelWriterOutputView(this.probeSideChannel, this.memorySegmentSize);
	}

	
	// --------------------------------------------------------------------------------------------------
	//                   Methods to provide input view abstraction for reading probe records
	// --------------------------------------------------------------------------------------------------
	
	public void setReadPosition(long pointer) {
		final int bufferNum = (int) (pointer >>> this.segmentSizeBits);
		final int offset = (int) (pointer & (this.memorySegmentSize - 1));
		
		this.currentBufferNum = bufferNum;
		seekInput(this.partitionBuffers[bufferNum], offset,
					bufferNum < this.partitionBuffers.length-1 ? this.memorySegmentSize : this.finalBufferLimit);
		
	}

	@Override
	protected MemorySegment nextSegment(MemorySegment current) throws IOException {
		this.currentBufferNum++;
		if (this.currentBufferNum < this.partitionBuffers.length) {
			return this.partitionBuffers[this.currentBufferNum];
		} else {
			throw new EOFException();
		}
	}


	@Override
	protected int getLimitForSegment(MemorySegment segment) {
		return segment == this.partitionBuffers[partitionBuffers.length - 1] ? this.finalBufferLimit : this.memorySegmentSize;
	}
	
	// ============================================================================================
	
	protected static final class BuildSideBuffer extends AbstractPagedOutputView {
		
		private final ArrayList<MemorySegment> targetList;
		
		private final MemorySegmentSource memSource;
		
		private BlockChannelWriter<MemorySegment> writer;
		
		private int currentBlockNumber;
		
		private final int sizeBits;
		
		
		private BuildSideBuffer(MemorySegment initialSegment, MemorySegmentSource memSource) {
			super(initialSegment, initialSegment.size(), 0);
			
			this.targetList = new ArrayList<MemorySegment>();
			this.memSource = memSource;
			this.sizeBits = MathUtils.log2strict(initialSegment.size());
		}
		

		@Override
		protected MemorySegment nextSegment(MemorySegment current, int bytesUsed) throws IOException {
			finalizeSegment(current, bytesUsed);
			
			final MemorySegment next;
			if (this.writer == null) {
				this.targetList.add(current);
				next = this.memSource.nextSegment();
			} else {
				this.writer.writeBlock(current);
				try {
					next = this.writer.getReturnQueue().take();
				} catch (InterruptedException iex) {
					throw new IOException("Hash Join Partition was interrupted while grabbing a new write-behind buffer.");
				}
			}
			
			this.currentBlockNumber++;
			return next;
		}
		
		long getCurrentPointer() {
			return (((long) this.currentBlockNumber) << this.sizeBits) + getCurrentPositionInSegment();
		}
		
		int getBlockCount() {
			return this.currentBlockNumber + 1;
		}

		int getNumOccupiedMemorySegments() {
			// return the current segment + all filled segments
			return this.targetList.size() + 1;
		}
		
		int spill(BlockChannelWriter<MemorySegment> writer) throws IOException {
			this.writer = writer;
			final int numSegments = this.targetList.size();
			for (int i = 0; i < numSegments; i++) {
				this.writer.writeBlock(this.targetList.get(i));
			}
			this.targetList.clear();
			return numSegments;
		}
		
		MemorySegment[] close() throws IOException {
			final MemorySegment current = getCurrentSegment();
			if (current == null) {
				throw new IllegalStateException("Illegal State in HashPartition: No current buffer when finilizing build side.");
			}
			finalizeSegment(current, getCurrentPositionInSegment());
			clear();
			
			if (this.writer == null) {
				this.targetList.add(current);
				MemorySegment[] buffers = this.targetList.toArray(new MemorySegment[this.targetList.size()]);
				this.targetList.clear();
				return buffers;
			} else {
				writer.writeBlock(current);
				return null;
			}
		}
		
		private void finalizeSegment(MemorySegment seg, int bytesUsed) {}
	}
	
	// ============================================================================================
	
	final class PartitionIterator implements MutableObjectIterator<BT> {
		
		private final TypeComparator<BT> comparator;
		
		private long currentPointer;
		
		private int currentHashCode;
		
		private PartitionIterator(final TypeComparator<BT> comparator) throws IOException {
			this.comparator = comparator;
			setReadPosition(0);
		}
		
		
		public final BT next(BT reuse) throws IOException {
			final int pos = getCurrentPositionInSegment();
			final int buffer = HashPartition.this.currentBufferNum;
			
			this.currentPointer = (((long) buffer) << HashPartition.this.segmentSizeBits) + pos;
			
			try {
				reuse = HashPartition.this.buildSideSerializer.deserialize(reuse, HashPartition.this);
				this.currentHashCode = this.comparator.hash(reuse);
				return reuse;
			} catch (EOFException eofex) {
				return null;
			}
		}

		public final BT next() throws IOException {
			final int pos = getCurrentPositionInSegment();
			final int buffer = HashPartition.this.currentBufferNum;

			this.currentPointer = (((long) buffer) << HashPartition.this.segmentSizeBits) + pos;

			try {
				BT result = HashPartition.this.buildSideSerializer.deserialize(HashPartition.this);
				this.currentHashCode = this.comparator.hash(result);
				return result;
			} catch (EOFException eofex) {
				return null;
			}
		}

		protected final long getPointer() {
			return this.currentPointer;
		}
		
		protected final int getCurrentHashCode() {
			return this.currentHashCode;
		}
	}
}
