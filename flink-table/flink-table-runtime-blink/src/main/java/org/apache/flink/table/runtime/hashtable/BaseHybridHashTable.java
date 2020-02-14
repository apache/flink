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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.compression.BlockCompressionFactory;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelReader;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.HeaderlessChannelReaderInputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.runtime.util.FileChannelUtil;
import org.apache.flink.table.runtime.util.MemorySegmentPool;
import org.apache.flink.util.MathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledSegment;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Base table for {@link LongHybridHashTable} and {@link BinaryHashTable}.
 */
public abstract class BaseHybridHashTable implements MemorySegmentPool {

	protected static final Logger LOG = LoggerFactory.getLogger(BaseHybridHashTable.class);

	/**
	 * The maximum number of recursive partitionings that the join does before giving up.
	 */
	protected static final int MAX_RECURSION_DEPTH = 3;

	/**
	 * The maximum number of partitions, which defines the spilling granularity. Each recursion,
	 * the data is divided maximally into that many partitions, which are processed in one chuck.
	 */
	protected static final int MAX_NUM_PARTITIONS = Byte.MAX_VALUE;

	/**
	 * The minimum number of memory segments the hash join needs to be supplied with in order to
	 * work.
	 */
	private static final int MIN_NUM_MEMORY_SEGMENTS = 33;
	protected final int initPartitionFanOut;

	private final int avgRecordLen;
	protected final long buildRowCount;

	/**
	 * The total reserved number of memory segments available to the hash join.
	 */
	protected final int totalNumBuffers;

	private final MemoryManager memManager;

	/**
	 * The free memory segments currently available to the hash join.
	 */
	public final ArrayList<MemorySegment> availableMemory;

	/**
	 * The I/O manager used to instantiate writers for the spilled partitions.
	 */
	protected final IOManager ioManager;

	/**
	 * The size of the segments used by the hash join buckets. All segments must be of equal size to
	 * ease offset computations.
	 */
	protected final int segmentSize;

	/**
	 * The queue of buffers that can be used for write-behind. Any buffer that is written
	 * asynchronously to disk is returned through this queue. hence
	 */
	protected final LinkedBlockingQueue<MemorySegment> buildSpillReturnBuffers;

	public final int segmentSizeBits;

	public final int segmentSizeMask;

	/**
	 * Flag indicating that the closing logic has been invoked.
	 */
	protected AtomicBoolean closed = new AtomicBoolean();

	/**
	 * Try to make the buildSide rows distinct.
	 */
	public final boolean tryDistinctBuildRow;

	/**
	 * The recursion depth of the partition that is currently processed. The initial table
	 * has a recursion depth of 0. Partitions spilled from a table that is built for a partition
	 * with recursion depth <i>n</i> have a recursion depth of <i>n+1</i>.
	 */
	protected int currentRecursionDepth;

	/**
	 * The number of buffers in the build spill return buffer queue that are actually not write behind buffers,
	 * but regular buffers that only have not yet returned. This is part of an optimization that the
	 * spilling code needs not wait until the partition is completely spilled before proceeding.
	 */
	protected int buildSpillRetBufferNumbers;

	/**
	 * The reader for the spilled-file of the build partition that is currently read.
	 */
	protected HeaderlessChannelReaderInputView currentSpilledBuildSide;
	/**
	 * The reader for the spilled-file of the probe partition that is currently read.
	 */
	protected AbstractChannelReaderInputView currentSpilledProbeSide;

	/**
	 * The channel enumerator that is used while processing the current partition to create
	 * channels for the spill partitions it requires.
	 */
	protected FileIOChannel.Enumerator currentEnumerator;

	protected final boolean compressionEnable;
	protected final BlockCompressionFactory compressionCodecFactory;
	protected final int compressionBlockSize;

	protected transient long numSpillFiles;
	protected transient long spillInBytes;

	public BaseHybridHashTable(
			Configuration conf,
			Object owner,
			MemoryManager memManager,
			long reservedMemorySize,
			IOManager ioManager,
			int avgRecordLen,
			long buildRowCount,
			boolean tryDistinctBuildRow) {

		//TODO: read compression config from configuration
		this.compressionEnable = conf.getBoolean(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED);
		this.compressionCodecFactory = this.compressionEnable
				? BlockCompressionFactory.createBlockCompressionFactory(BlockCompressionFactory.CompressionFactoryName.LZ4.toString())
				: null;
		this.compressionBlockSize = (int) MemorySize.parse(
			conf.getString(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)).getBytes();

		this.avgRecordLen = avgRecordLen;
		this.buildRowCount = buildRowCount;
		this.tryDistinctBuildRow = tryDistinctBuildRow;
		this.totalNumBuffers = (int) (reservedMemorySize / memManager.getPageSize());
		// some sanity checks first
		checkArgument(totalNumBuffers >= MIN_NUM_MEMORY_SEGMENTS);
		this.availableMemory = new ArrayList<>(this.totalNumBuffers);
		try {
			List<MemorySegment> allocates = memManager.allocatePages(owner, totalNumBuffers);
			this.availableMemory.addAll(allocates);
			allocates.clear();
		} catch (MemoryAllocationException e) {
			LOG.error("Out of memory", e);
			throw new RuntimeException(e);
		}
		this.memManager = memManager;
		this.ioManager = ioManager;

		this.segmentSize = memManager.getPageSize();

		checkArgument(MathUtils.isPowerOf2(segmentSize));

		// take away the write behind buffers
		this.buildSpillReturnBuffers = new LinkedBlockingQueue<>();

		this.segmentSizeBits = MathUtils.log2strict(segmentSize);
		this.segmentSizeMask = segmentSize - 1;

		// open builds the initial table by consuming the build-side input
		this.currentRecursionDepth = 0;

		// create the partitions
		this.initPartitionFanOut = Math.min(getPartitioningFanOutNoEstimates(), maxNumPartition());

		this.closed.set(false);

		LOG.info(String.format("Initialize hash table with %d memory segments, each size [%d], the memory %d MB.",
				totalNumBuffers, segmentSize, (long) totalNumBuffers * segmentSize / 1024 / 1024));
	}

	/**
	 * Bucket area need at-least one and data need at-least one.
	 * In the initialization phase, we can use (totalNumBuffers - numWriteBehindBuffers) Segments.
	 * However, in the buildTableFromSpilledPartition phase, only (totalNumBuffers - numWriteBehindBuffers - 2)
	 * can be used because two Buffers are needed to read the data.
	 */
	protected int maxNumPartition() {
		return (availableMemory.size() + buildSpillRetBufferNumbers) / 2;
	}

	/**
	 * Gets the number of partitions to be used for an initial hash-table.
	 */
	private int getPartitioningFanOutNoEstimates() {
		return Math.max(11, findSmallerPrime((int) Math.min(buildRowCount * avgRecordLen / (10 * segmentSize),
				MAX_NUM_PARTITIONS)));
	}

	/**
	 * Let prime number be the numBuckets, to avoid partition hash and bucket hash congruences.
	 */
	private static int findSmallerPrime(int num) {
		for (; num > 1; num--) {
			if (isPrimeNumber(num)) {
				return num;
			}
		}
		return num;
	}

	private static boolean isPrimeNumber(int num){
		if (num == 2) {
			return true;
		}
		if (num < 2 || num % 2 == 0) {
			return false;
		}
		for (int i = 3; i <= Math.sqrt(num); i += 2){
			if (num % i == 0) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Gets the next buffer to be used with the hash-table, either for an in-memory partition, or
	 * for the table buckets. This method returns <tt>null</tt>, if no more buffer is available.
	 * Spilling a partition may free new buffers then.
	 *
	 * @return The next buffer to be used by the hash-table, or null, if no buffer remains.
	 */
	public MemorySegment getNextBuffer() {
		// check if the list directly offers memory
		int s = this.availableMemory.size();
		if (s > 0) {
			return this.availableMemory.remove(s - 1);
		}

		// check if there are write behind buffers that actually are to be used for the hash table
		if (this.buildSpillRetBufferNumbers > 0) {
			// grab at least one, no matter what
			MemorySegment toReturn;
			try {
				toReturn = this.buildSpillReturnBuffers.take();
			} catch (InterruptedException iex) {
				throw new RuntimeException("Hybrid Hash Join was interrupted while taking a buffer.");
			}
			this.buildSpillRetBufferNumbers--;

			// grab as many more buffers as are available directly
			MemorySegment currBuff;
			while (this.buildSpillRetBufferNumbers > 0 && (currBuff = this.buildSpillReturnBuffers.poll()) != null) {
				this.availableMemory.add(currBuff);
				this.buildSpillRetBufferNumbers--;
			}
			return toReturn;
		} else {
			return null;
		}
	}

	/**
	 * Bulk memory acquisition.
	 * NOTE: Failure to get memory will throw an exception.
	 */
	public MemorySegment[] getNextBuffers(int bufferSize) {
		MemorySegment[] memorySegments = new MemorySegment[bufferSize];
		for (int i = 0; i < bufferSize; i++) {
			MemorySegment nextBuffer = getNextBuffer();
			if (nextBuffer == null) {
				throw new RuntimeException("No enough buffers!");
			}
			memorySegments[i] = nextBuffer;
		}
		return memorySegments;
	}

	protected MemorySegment getNotNullNextBuffer() {
		MemorySegment buffer = getNextBuffer();
		if (buffer == null) {
			throw new RuntimeException("Bug in HybridHashJoin: No memory became available.");
		}
		return buffer;
	}

	/**
	 * This is the method called by the partitions to request memory to serialize records.
	 * It automatically spills partitions, if memory runs out.
	 *
	 * @return The next available memory segment.
	 */
	@Override
	public MemorySegment nextSegment() {
		final MemorySegment seg = getNextBuffer();
		if (seg != null) {
			return seg;
		} else {
			try {
				spillPartition();
			} catch (IOException ioex) {
				throw new RuntimeException("Error spilling Hash Join Partition" + (ioex.getMessage() == null ?
						"." : ": " + ioex.getMessage()), ioex);
			}

			MemorySegment fromSpill = getNextBuffer();
			if (fromSpill == null) {
				throw new RuntimeException("BUG in Hybrid Hash Join: Spilling did not free a buffer.");
			} else {
				return fromSpill;
			}
		}
	}

	@Override
	public int pageSize() {
		return segmentSize;
	}

	@Override
	public void returnAll(List<MemorySegment> memory) {
		for (MemorySegment segment : memory) {
			if (segment != null) {
				availableMemory.add(segment);
			}
		}
	}

	protected abstract int spillPartition() throws IOException;

	/**
	 * This method makes sure that at least a certain number of memory segments is in the list of
	 * free segments.
	 * Free memory can be in the list of free segments, or in the return-queue where segments used
	 * to write behind are
	 * put. The number of segments that are in that return-queue, but are actually reclaimable is
	 * tracked. This method
	 * makes sure at least a certain number of buffers is reclaimed.
	 *
	 * @param minRequiredAvailable The minimum number of buffers that needs to be reclaimed.
	 */
	public void ensureNumBuffersReturned(final int minRequiredAvailable) {
		if (minRequiredAvailable > this.availableMemory.size() + this.buildSpillRetBufferNumbers) {
			throw new IllegalArgumentException("More buffers requested available than totally available.");
		}

		try {
			while (this.availableMemory.size() < minRequiredAvailable) {
				this.availableMemory.add(this.buildSpillReturnBuffers.take());
				this.buildSpillRetBufferNumbers--;
			}
		} catch (InterruptedException iex) {
			throw new RuntimeException("Hash Join was interrupted.");
		}
	}

	/**
	 * Closes the hash table. This effectively releases all internal structures and closes all
	 * open files and removes them. The call to this method is valid both as a cleanup after the
	 * complete inputs were properly processed, and as an cancellation call, which cleans up
	 * all resources that are currently held by the hash join.
	 */
	public void close() {
		// make sure that we close only once
		if (!this.closed.compareAndSet(false, true)) {
			return;
		}

		// clear the current probe side channel, if there is one
		if (this.currentSpilledProbeSide != null) {
			try {
				this.currentSpilledProbeSide.getChannel().closeAndDelete();
			} catch (Throwable t) {
				LOG.warn("Could not close and delete the temp file for the current spilled partition probe side.", t);
			}
		}

		// clear the memory in the partitions
		clearPartitions();

		// return the write-behind buffers
		for (int i = 0; i < this.buildSpillRetBufferNumbers; i++) {
			try {
				this.availableMemory.add(this.buildSpillReturnBuffers.take());
			} catch (InterruptedException iex) {
				throw new RuntimeException("Hashtable closing was interrupted");
			}
		}
		this.buildSpillRetBufferNumbers = 0;
	}

	protected abstract void clearPartitions();

	public void free() {
		if (this.closed.get()) {
			memManager.release(availableMemory);
		} else {
			throw new IllegalStateException("Cannot release memory until BinaryHashTable is closed!");
		}
	}

	/**
	 * Free the memory not used.
	 */
	public void freeCurrent() {
		memManager.release(availableMemory);
	}

	@VisibleForTesting
	public List<MemorySegment> getFreedMemory() {
		return this.availableMemory;
	}

	public void free(MemorySegment segment) {
		this.availableMemory.add(segment);
	}

	public int remainBuffers() {
		return availableMemory.size() + buildSpillRetBufferNumbers;
	}

	public long getUsedMemoryInBytes() {
		return (totalNumBuffers - availableMemory.size()) * ((long) memManager.getPageSize());
	}

	public long getNumSpillFiles() {
		return numSpillFiles;
	}

	public long getSpillInBytes() {
		return spillInBytes;
	}

	/**
	 * Give up to one-sixth of the memory of the bucket area.
	 */
	public int maxInitBufferOfBucketArea(int partitions) {
		return Math.max(1, ((totalNumBuffers - 2) / 6) / partitions);
	}

	protected List<MemorySegment> readAllBuffers(FileIOChannel.ID id, int blockCount) throws IOException {
		// we are guaranteed to stay in memory
		ensureNumBuffersReturned(blockCount);

		LinkedBlockingQueue<MemorySegment> retSegments = new LinkedBlockingQueue<>();
		BlockChannelReader<MemorySegment> reader = FileChannelUtil.createBlockChannelReader(
				ioManager, id, retSegments,
				compressionEnable, compressionCodecFactory, compressionBlockSize, segmentSize);
		for (int i = 0; i < blockCount; i++) {
			reader.readBlock(availableMemory.remove(availableMemory.size() - 1));
		}
		reader.closeAndDelete();

		final List<MemorySegment> buffers = new ArrayList<>();
		retSegments.drainTo(buffers);
		return buffers;
	}

	protected HeaderlessChannelReaderInputView createInputView(FileIOChannel.ID id, int blockCount, int lastSegmentLimit) throws IOException {
		BlockChannelReader<MemorySegment> inReader = FileChannelUtil.createBlockChannelReader(
				ioManager, id, new LinkedBlockingQueue<>(),
				compressionEnable, compressionCodecFactory, compressionBlockSize, segmentSize);
		return new HeaderlessChannelReaderInputView(inReader,
													Arrays.asList(allocateUnpooledSegment(segmentSize), allocateUnpooledSegment(segmentSize)),
													blockCount, lastSegmentLimit, false);

	}

	/**
	 * The level parameter is needed so that we can have different hash functions when we
	 * recursively apply the partitioning, so that the working set eventually fits into memory.
	 */
	public static int hash(int hashCode, int level) {
		final int rotation = level * 11;
		int code = Integer.rotateLeft(hashCode, rotation);
		return code >= 0 ? code : -(code + 1);
	}

	/**
	 * Partition level hash again, for avoid two layer hash conflict.
	 */
	static int partitionLevelHash(int hash) {
		return hash ^ (hash >>> 16);
	}
}
