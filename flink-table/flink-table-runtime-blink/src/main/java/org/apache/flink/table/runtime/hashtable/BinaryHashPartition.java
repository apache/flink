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

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentSource;
import org.apache.flink.core.memory.SeekableDataInputView;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.RandomAccessInputView;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.BulkBlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.runtime.typeutils.BinaryRowSerializer;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.table.runtime.util.RowIterator;
import org.apache.flink.util.MathUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A partition.
 */
public class BinaryHashPartition extends AbstractPagedInputView implements SeekableDataInputView {

	private final BinaryRowSerializer buildSideSerializer;
	private final BinaryRowSerializer probeSideSerializer;
	private final int segmentSizeBits; // the number of bits in the mem segment size;

	private boolean compressionEnable;
	private BlockCompressionFactory compressionCodecFactory;
	private int compressionBlockSize;

	private final int memorySegmentSize; // the size of the memory segments being used
	final int partitionNumber; // the number of the partition

	long probeSideRecordCounter; // number of probe-side records in this partition
	private MemorySegment[] partitionBuffers;
	private int currentBufferNum;
	private int finalBufferLimit;

	private BuildSideBuffer buildSideWriteBuffer;
	AbstractChannelWriterOutputView probeSideBuffer;
	private long buildSideRecordCounter; // number of build-side records in this partition
	private int recursionLevel; // the recursion level on which this partition lives

	// the channel writer for the build side, if partition is spilled
	private BlockChannelWriter<MemorySegment> buildSideChannel;

	// bucket area of this partition
	BinaryHashBucketArea bucketArea;

	/**
	 * The bloom filter utility used to transform spilled partitions into a
	 * probabilistic filter.
	 */
	HashTableBloomFilter bloomFilter;

	private MemorySegmentPool memPool;

	int probeNumBytesInLastSeg;

	/**
	 * Creates a new partition, initially in memory, with one buffer for the build side. The
	 * partition is initialized to expect record insertions for the build side.
	 *
	 * @param partitionNumber The number of the partition.
	 * @param recursionLevel  The recursion level - zero for partitions from the initial build,
	 *                        <i>n + 1</i> for partitions that are created from spilled partition
	 *                        with recursion level <i>n</i>.
	 * @param initialBuffer   The initial buffer for this partition.
	 */
	BinaryHashPartition(BinaryHashBucketArea bucketArea, BinaryRowSerializer buildSideAccessors, BinaryRowSerializer probeSideAccessors,
						int partitionNumber, int recursionLevel, MemorySegment initialBuffer,
						MemorySegmentPool memPool, int segmentSize, boolean compressionEnable,
						BlockCompressionFactory compressionCodecFactory, int compressionBlockSize) {
		super(0);
		this.bucketArea = bucketArea;
		this.buildSideSerializer = buildSideAccessors;
		this.probeSideSerializer = probeSideAccessors;
		this.partitionNumber = partitionNumber;
		this.recursionLevel = recursionLevel;
		this.memorySegmentSize = segmentSize;
		this.segmentSizeBits = MathUtils.log2strict(segmentSize);
		this.compressionEnable = compressionEnable;
		this.compressionCodecFactory = compressionCodecFactory;
		this.compressionBlockSize = compressionBlockSize;
		this.buildSideWriteBuffer = new BuildSideBuffer(initialBuffer, memPool);
		this.memPool = memPool;
	}

	/**
	 * Constructor creating a partition from a spilled partition file that could be read in one
	 * because it was known to completely fit into memory.
	 *
	 * @param buildSideAccessors     The data type accessors for the build side data-type.
	 * @param probeSideAccessors     The data type accessors for the probe side data-type.
	 * @param partitionNumber        The number of the partition.
	 * @param recursionLevel         The recursion level of the partition.
	 * @param buffers                The memory segments holding the records.
	 * @param buildSideRecordCounter The number of records in the buffers.
	 * @param segmentSize            The size of the memory segments.
	 */
	BinaryHashPartition(BinaryHashBucketArea area, BinaryRowSerializer buildSideAccessors, BinaryRowSerializer probeSideAccessors,
						int partitionNumber, int recursionLevel, List<MemorySegment> buffers,
						long buildSideRecordCounter, int segmentSize, int lastSegmentLimit) {
		super(0);
		this.buildSideSerializer = buildSideAccessors;
		this.probeSideSerializer = probeSideAccessors;
		this.partitionNumber = partitionNumber;
		this.recursionLevel = recursionLevel;

		this.memorySegmentSize = segmentSize;
		this.segmentSizeBits = MathUtils.log2strict(segmentSize);
		this.finalBufferLimit = lastSegmentLimit;

		this.partitionBuffers = buffers.toArray(new MemorySegment[buffers.size()]);
		this.buildSideRecordCounter = buildSideRecordCounter;

		this.bucketArea = area;
	}

	/**
	 * Gets the partition number of this partition.
	 *
	 * @return This partition's number.
	 */
	int getPartitionNumber() {
		return this.partitionNumber;
	}

	/**
	 * Gets this partition's recursion level.
	 *
	 * @return The partition's recursion level.
	 */
	int getRecursionLevel() {
		return this.recursionLevel;
	}

	/**
	 * Checks whether this partition is in memory or spilled.
	 *
	 * @return True, if the partition is in memory, false if it is spilled.
	 */
	final boolean isInMemory() {
		return this.buildSideChannel == null;
	}

	/**
	 * Gets the number of memory segments used by this partition, which includes build side
	 * memory buffers and overflow memory segments.
	 *
	 * @return The number of occupied memory segments.
	 */
	int getNumOccupiedMemorySegments() {
		// either the number of memory segments, or one for spilling
		final int numPartitionBuffers = this.partitionBuffers != null ?
				this.partitionBuffers.length
				: this.buildSideWriteBuffer.getNumOccupiedMemorySegments();
		return numPartitionBuffers + bucketArea.buckets.length + bucketArea.numOverflowSegments;
	}

	int getBuildSideBlockCount() {
		return this.partitionBuffers == null ? this.buildSideWriteBuffer.getBlockCount()
				: this.partitionBuffers.length;
	}

	RandomAccessInputView getBuildStateInputView() {
		return this.buildSideWriteBuffer.getBuildStageInputView();
	}

	int getProbeSideBlockCount() {
		return this.probeSideBuffer == null ? -1 : this.probeSideBuffer.getBlockCount();
	}

	long getBuildSideRecordCount() {
		return this.buildSideRecordCounter;
	}

	BlockChannelWriter<MemorySegment> getBuildSideChannel() {
		return this.buildSideChannel;
	}

	boolean testHashBloomFilter(int hash) {
		return bloomFilter == null || bloomFilter.testHash(hash);
	}

	private void freeBloomFilter() {
		memPool.returnAll(Arrays.asList(bloomFilter.getBuffers()));
		bloomFilter = null;
	}

	/**
	 * Add new hash to bloomFilter when insert a record to spilled partition.
	 */
	void addHashBloomFilter(int hash) {
		if (bloomFilter != null) {
			// check if too full.
			if (!bloomFilter.addHash(hash)) {
				freeBloomFilter();
			}
		}
	}

	/**
	 * Inserts the given object into the current buffer. This method returns a pointer that
	 * can be used to address the written record in this partition, if it is in-memory.
	 * The returned pointers have no expressiveness in the case where the partition is spilled.
	 *
	 * @param record The object to be written to the partition.
	 * @return A pointer to the object in the partition, or <code>-1</code>, if the partition is
	 *         spilled.
	 * @throws IOException Thrown, when this is a spilled partition and the write failed.
	 */
	final int insertIntoBuildBuffer(BinaryRow record) throws IOException {
		this.buildSideRecordCounter++;

		if (isInMemory()) {
			final long pointer = this.buildSideWriteBuffer.getCurrentPointer();
			int skip = this.buildSideSerializer.serializeToPages(record, this.buildSideWriteBuffer);
			if (isInMemory()) {
				long ret = pointer + skip;
				if (ret > Integer.MAX_VALUE) {
					throw new RuntimeException("Too more data in this partition: " + ret);
				}
				return (int) ret;
			} else {
				return -1;
			}
		} else {
			this.buildSideSerializer.serializeToPages(record, this.buildSideWriteBuffer);
			return -1;
		}
	}

	/**
	 * Inserts the given record into the probe side buffers. This method is only applicable when
	 * the partition was spilled while processing the build side.
	 *
	 * <p>If this method is invoked when the partition is still being built, it has undefined
	 * behavior.
	 *
	 * @param record The record to be inserted into the probe side buffers.
	 * @throws IOException Thrown, if the buffer is full, needs to be spilled, and spilling causes
	 *                     an error.
	 */
	final void insertIntoProbeBuffer(BinaryRow record) throws IOException {
		this.probeSideSerializer.serialize(record, this.probeSideBuffer);
		this.probeSideRecordCounter++;
	}

	/**
	 * Spills this partition to disk and sets it up such that it continues spilling records that are
	 * added to
	 * it. The spilling process must free at least one buffer, either in the partition's record
	 * buffers, or in
	 * the memory segments for overflow buckets.
	 * The partition immediately takes back one buffer to use it for further spilling.
	 *
	 * @param ioAccess      The I/O manager to be used to create a writer to disk.
	 * @param targetChannel The id of the target channel for this partition.
	 * @return The number of buffers that were freed by spilling this partition.
	 * @throws IOException Thrown, if the writing failed.
	 */
	int spillPartition(
			IOManager ioAccess, FileIOChannel.ID targetChannel,
			LinkedBlockingQueue<MemorySegment> bufferReturnQueue) throws IOException {
		// sanity checks
		if (!isInMemory()) {
			throw new RuntimeException("Bug in Hybrid Hash Join: " +
					"Request to spill a partition that has already been spilled.");
		}
		if (getNumOccupiedMemorySegments() < 2) {
			throw new RuntimeException("Bug in Hybrid Hash Join: " +
					"Request to spill a partition with less than two buffers.");
		}

		// create the channel block writer and spill the current buffers
		// that keep the build side buffers current block, as it is most likely not full, yet
		// we return the number of blocks that become available
		this.buildSideChannel = FileChannelUtil.createBlockChannelWriter(
				ioAccess,
				targetChannel,
				bufferReturnQueue,
				compressionEnable,
				compressionCodecFactory,
				compressionBlockSize,
				memorySegmentSize);
		return this.buildSideWriteBuffer.spill(this.buildSideChannel);
	}

	void buildBloomFilterAndFreeBucket() {
		if (bucketArea != null) {
			this.bucketArea.buildBloomFilterAndFree();
			this.bucketArea = null;
		}
	}

	/**
	 * After build phase.
	 * @return build spill return buffer, if have spilled, it returns the current write buffer,
	 * because it was used all the time in build phase, so it can only be returned at this time.
	 */
	int finalizeBuildPhase(IOManager ioAccess, FileIOChannel.Enumerator probeChannelEnumerator) throws IOException {
		this.finalBufferLimit = this.buildSideWriteBuffer.getCurrentPositionInSegment();
		this.partitionBuffers = this.buildSideWriteBuffer.close();

		if (!isInMemory()) {
			// close the channel.
			this.buildSideChannel.close();

			this.probeSideBuffer = FileChannelUtil.createOutputView(
					ioAccess,
					probeChannelEnumerator.next(),
					compressionEnable,
					compressionCodecFactory,
					compressionBlockSize,
					memorySegmentSize);
			return 1;
		} else {
			return 0;
		}
	}

	/**
	 * @param keepUnprobedSpilledPartitions If true then partitions that were spilled but received
	 *                                      no further probe
	 *                                      requests will be retained; used for build-side outer
	 *                                      joins.
	 */
	void finalizeProbePhase(List<MemorySegment> freeMemory, List<BinaryHashPartition> spilledPartitions,
							boolean keepUnprobedSpilledPartitions) throws IOException {
		if (isInMemory()) {
			this.bucketArea.returnMemory(freeMemory);
			this.bucketArea = null;
			// return the partition buffers
			Collections.addAll(freeMemory, this.partitionBuffers);
			this.partitionBuffers = null;
		} else {
			if (bloomFilter != null) {
				freeBloomFilter();
			}
			if (this.probeSideRecordCounter == 0 && !keepUnprobedSpilledPartitions) {
				// delete the spill files
				this.probeSideBuffer.close();
				this.buildSideChannel.deleteChannel();
				this.probeSideBuffer.getChannel().deleteChannel();
			} else {
				// flush the last probe side buffer and register this partition as pending
				probeNumBytesInLastSeg = this.probeSideBuffer.close();
				spilledPartitions.add(this);
			}
		}
	}

	void clearAllMemory(List<MemorySegment> target) {
		// return current buffers from build side and probe side
		if (this.buildSideWriteBuffer != null) {
			if (this.buildSideWriteBuffer.getCurrentSegment() != null) {
				target.add(this.buildSideWriteBuffer.getCurrentSegment());
			}
			target.addAll(this.buildSideWriteBuffer.targetList);
			this.buildSideWriteBuffer.targetList.clear();
			this.buildSideWriteBuffer = null;
		}

		// return the overflow segments
		if (this.bucketArea != null) {
			bucketArea.returnMemory(target);
		}

		if (bloomFilter != null) {
			freeBloomFilter();
		}

		// return the partition buffers
		if (this.partitionBuffers != null) {
			Collections.addAll(target, this.partitionBuffers);
			this.partitionBuffers = null;
		}

		// clear the channels
		try {
			if (this.buildSideChannel != null) {
				this.buildSideChannel.close();
				this.buildSideChannel.deleteChannel();
			}
			if (this.probeSideBuffer != null) {
				this.probeSideBuffer.close();
				this.probeSideBuffer.getChannel().deleteChannel();
				this.probeSideBuffer = null;
			}
		} catch (IOException ioex) {
			throw new RuntimeException("Error deleting the partition files. " +
					"Some temporary files might not be removed.", ioex);
		}
	}

	final PartitionIterator newPartitionIterator() {
		return new PartitionIterator();
	}

	final int getLastSegmentLimit() {
		return this.finalBufferLimit;
	}

	@Override
	public void setReadPosition(long pointer) {
		final int bufferNum = (int) (pointer >>> this.segmentSizeBits);
		final int offset = (int) (pointer & (this.memorySegmentSize - 1));

		this.currentBufferNum = bufferNum;
		seekInput(this.partitionBuffers[bufferNum], offset,
				bufferNum < this.partitionBuffers.length - 1 ? this.memorySegmentSize
						: this.finalBufferLimit);

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
		return segment == this.partitionBuffers[partitionBuffers.length - 1]
				? this.finalBufferLimit : this.memorySegmentSize;
	}

	/**
	 * Build side buffer.
	 */
	protected static final class BuildSideBuffer extends AbstractPagedOutputView {

		private final ArrayList<MemorySegment> targetList;
		/**
		 * Segments of in memory partition, include the current segment.
		 * Cleared after spilled.
		 */
		private final ArrayList<MemorySegment> buildStageSegments;
		private final RandomAccessInputView buildStageInputView;

		private final MemorySegmentSource memSource;
		private final int sizeBits;
		private BlockChannelWriter<MemorySegment> writer;
		private int currentBlockNumber;

		private BuildSideBuffer(MemorySegment initialSegment, MemorySegmentSource memSource) {
			super(initialSegment, initialSegment.size(), 0);

			this.memSource = memSource;
			this.sizeBits = MathUtils.log2strict(initialSegment.size());
			this.targetList = new ArrayList<>();
			this.buildStageSegments = new ArrayList<>();
			this.buildStageSegments.add(initialSegment);
			this.buildStageInputView = new RandomAccessInputView(
					buildStageSegments, initialSegment.size());
		}

		@Override
		protected MemorySegment nextSegment(
				MemorySegment current,
				int bytesUsed) throws IOException {
			final MemorySegment next;
			if (this.writer == null) {
				// Must first add current segment:
				// This may happen when you need to spill:
				// A partition called nextSegment, can not get memory, need to spill, the result
				// give itself to the spill, Since it is switching currentSeg, it is necessary
				// to give the previous currSeg to spill.
				this.targetList.add(current);
				next = this.memSource.nextSegment();
				buildStageSegments.add(next);
			} else {
				this.writer.writeBlock(current);
				try {
					next = this.writer.getReturnQueue().take();
				} catch (InterruptedException iex) {
					throw new IOException("Hash Join Partition was interrupted while " +
							"grabbing a new write-behind buffer.");
				}
			}

			this.currentBlockNumber++;
			return next;
		}

		RandomAccessInputView getBuildStageInputView() {
			return buildStageInputView;
		}

		long getCurrentPointer() {
			return (((long) this.currentBlockNumber) << this.sizeBits)
					+ getCurrentPositionInSegment();
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
			for (MemorySegment segment : this.targetList) {
				this.writer.writeBlock(segment);
			}
			this.targetList.clear();
			return numSegments;
		}

		MemorySegment[] close() throws IOException {
			final MemorySegment current = getCurrentSegment();
			if (current == null) {
				throw new IllegalStateException("Illegal State in HashPartition: " +
						"No current buffer when finalizing build side.");
			}
			clear();

			if (this.writer == null) {
				this.targetList.add(current);
				MemorySegment[] buffers =
						this.targetList.toArray(new MemorySegment[this.targetList.size()]);
				this.targetList.clear();
				this.buildStageSegments.clear();
				return buffers;
			} else {
				writer.writeBlock(current);
				return null;
			}
		}
	}

	/**
	 * For spilled partition to rebuild index and hashcode when memory can
	 * store all the build side data.
	 * (After bulk load to memory, see {@link BulkBlockChannelReader}).
	 */
	final class PartitionIterator implements RowIterator<BinaryRow> {

		private long currentPointer;

		private BinaryRow reuse;

		private PartitionIterator() {
			this.reuse = buildSideSerializer.createInstance();
			setReadPosition(0);
		}

		@Override
		public boolean advanceNext() {
			final int pos = getCurrentPositionInSegment();
			final int buffer = BinaryHashPartition.this.currentBufferNum;

			this.currentPointer = (((long) buffer) << segmentSizeBits) + pos;

			try {
				reuse = buildSideSerializer.mapFromPages(reuse, BinaryHashPartition.this);
				return true;
			} catch (EOFException e) {
				return false;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		final long getPointer() {
			return this.currentPointer;
		}

		@Override
		public BinaryRow getRow() {
			return this.reuse;
		}
	}
}
