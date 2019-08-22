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

package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.map.LinkedMap;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.runtime.state.heap.space.SpaceConstants.NO_SPACE;

/**
 * This is a simple buddy-like allocator used for small space. The allocator always allocates a block
 * of power-of-two size to satisfy a space request. For example, if a space of 54 bytes is requested,
 * allocator will allocate a block of 64 bytes to it which leads to a waste of 10 bytes.
 *
 * <p>A {@link Chunk} will be divided into multiple buckets of the same size. Each bucket is further
 * divided into blocks. Blocks in a bucket have the same size, but blocks in different buckets may have
 * different size. Size of each block must be a power of two, and the maximum size of block can't exceed
 * bucket size.
 */
public class PowerTwoBucketAllocator implements BucketAllocator {

	/**
	 * Minimum size of a block.
	 */
	private static final int MIN_BLOCK_SIZE = 32;

	/**
	 * Number of bits to represent {@link PowerTwoBucketAllocator#MIN_BLOCK_SIZE}.
	 */
	private static final int MIN_BLOCK_SIZE_BITS = MathUtils.log2strict(MIN_BLOCK_SIZE);

	/**
	 * Number of bits to represent bucket size.
	 */
	private final int bucketSizeBits;

	/**
	 * An array of all buckets.
	 */
	private Bucket[] allBuckets;

	/**
	 * An array of block allocator, and each allocator can only allocate blocks with fixed size.
	 * Size of blocks allocated by a allocator is twice of that allocated by the former allocator
	 * in the array. For example, size of blocks allocated by allocators in the array can be 32,
	 * 64, 128.
	 */
	private BlockAllocator[] blockAllocators;

	/**
	 * A queue of free buckets.
	 */
	private final ConcurrentLinkedQueue<Bucket> freeBucketsQueue;

	PowerTwoBucketAllocator(int chunkSize, int bucketSize) {
		Preconditions.checkArgument((bucketSize & bucketSize - 1) == 0,
			"Bucket size must be a power of 2, but the actual is " + bucketSize);
		this.bucketSizeBits = MathUtils.log2strict(bucketSize);
		this.freeBucketsQueue = new ConcurrentLinkedQueue<>();

		int numberBuckets = chunkSize >>> this.bucketSizeBits;
		// number of possible values for block size
		int numberBlockSize = bucketSizeBits - MIN_BLOCK_SIZE_BITS + 1;
		Preconditions.checkArgument(numberBuckets >= numberBlockSize,
			"Number of possible values for block size is more than the number of buckets, " +
				"so allocator can't satisfy requests for different block size at the same time. " +
				"Try to use a larger chunk.");
		this.allBuckets = new Bucket[numberBuckets];
		for (int i = 0; i < allBuckets.length; ++i) {
			allBuckets[i] = new Bucket(i << this.bucketSizeBits, bucketSize);
			this.freeBucketsQueue.offer(allBuckets[i]);
		}

		this.blockAllocators = new BlockAllocator[this.bucketSizeBits - MIN_BLOCK_SIZE_BITS + 1];
		for (int i = 0; i < blockAllocators.length; i++) {
			blockAllocators[i] = new BlockAllocator(i, this.freeBucketsQueue);
		}
	}

	@Override
	public int allocate(int len) {
		Preconditions.checkArgument(len > 0,
			"Size to allocate must be positive, but the actual is " + len);
		int blockAllocatorIndex = getBlockAllocatorIndex(len);
		if (blockAllocatorIndex >= blockAllocators.length) {
			throw new RuntimeException("PowerTwoBucketAllocator can't allocate size larger than bucket size");
		}
		return blockAllocators[blockAllocatorIndex].allocateBlock();
	}

	@Override
	public void free(int offset) {
		int bucketIndex = getBucketIndex(offset);
		Bucket bucket = allBuckets[bucketIndex];
		blockAllocators[bucket.getBlockAllocatorIndex()].freeBlock(bucket, offset);
	}

	@VisibleForTesting
	int getBucketIndex(int offset) {
		return offset >>> bucketSizeBits;
	}

	@VisibleForTesting
	Bucket[] getAllBuckets() {
		return allBuckets;
	}

	@VisibleForTesting
	BlockAllocator[] getBlockAllocators() {
		return blockAllocators;
	}

	@VisibleForTesting
	ConcurrentLinkedQueue<Bucket> getFreeBucketsQueue() {
		return freeBucketsQueue;
	}

	/**
	 * Returns the index of block allocator responsible for the given block size in
	 * {@link PowerTwoBucketAllocator#blockAllocators}.
	 */
	@VisibleForTesting
	static int getBlockAllocatorIndex(int blockSize) {
		Preconditions.checkArgument(blockSize > 0, "Block size must be positive");
		if (blockSize <= MIN_BLOCK_SIZE) {
			return 0;
		}
		return MathUtils.log2strict(MathUtils.roundUpToPowerOfTwo(blockSize)) - MIN_BLOCK_SIZE_BITS;
	}

	/**
	 * Returns block size of the block allocator with the given index.
	 */
	@VisibleForTesting
	static int getBlockSizeFromBlockAllocatorIndex(int blockAllocatorIndex) {
		return 1 << (blockAllocatorIndex + MIN_BLOCK_SIZE_BITS);
	}

	/**
	 * A bucket is a continuous space that is divided into blocks with fixed size.
	 * Buckets can be reused to allocate another size of blocks after it's freed.
	 */
	public static final class Bucket {

		/**
		 * Base offset of this bucket in the chunk.
		 */
		private final int baseOffset;

		/**
		 * Size of this bucket.
		 */
		private final int bucketSize;

		/**
		 * Index of block allocator which is using this bucket. -1 indicates
		 * this bucket is not used by any allocator.
		 */
		private int blockAllocatorIndex;

		/**
		 * Size of block to allocate. This field is only meaningful when the
		 * bucket is used by some allocator.
		 */
		private int blockSize;

		/**
		 * Number of bits to represent the block size.
		 */
		private int blockSizeBits;

		/**
		 * Number of free blocks in bucket.
		 */
		private int freeBlockNum;

		/**
		 * Number of used blocks in bucket.
		 */
		private int usedBlockNum;

		/**
		 * An array of indexes for free blocks. Blocks in [0, getFreeBlockNum)
		 * are free to use. This works like a stack. For allocate, last block
		 * is popped for the back of array, and for free, block are pushed to
		 * the back of array.
		 */
		private int[] freeBlockIndexes;

		Bucket(int offset, int bucketSize) {
			this.baseOffset = offset;
			this.bucketSize = bucketSize;
			this.blockAllocatorIndex = -1;
		}

		/**
		 * Reuse the bucket by the given allocator.
		 *
		 * @param bucketAllocatorIndex index of bucket allocator.
		 */
		void reuseBucket(int bucketAllocatorIndex) {
			this.blockAllocatorIndex = bucketAllocatorIndex;
			this.blockSize = getBlockSizeFromBlockAllocatorIndex(this.blockAllocatorIndex);
			Preconditions.checkArgument(this.blockSize <= this.bucketSize,
				"Block size can't be larger than bucket size.");
			this.blockSizeBits = MathUtils.log2strict(this.blockSize);

			int totalBlockNum = this.bucketSize >>> this.blockSizeBits;
			this.freeBlockNum = totalBlockNum;
			this.usedBlockNum = 0;
			this.freeBlockIndexes = new int[totalBlockNum];
			for (int i = 0; i < totalBlockNum; ++i) {
				this.freeBlockIndexes[i] = i;
			}
		}

		/**
		 * Allocates a block.
		 *
		 * @return offset of the block.
		 */
		public int allocate() {
			// pop the last block
			++usedBlockNum;
			--freeBlockNum;
			return baseOffset + (freeBlockIndexes[freeBlockNum] << blockSizeBits);
		}

		/**
		 * Frees the block with the given offset.
		 *
		 * @param offset offset of the block to free.
		 */
		public void free(int offset) {
			int bucketOffset = offset - baseOffset;
			int blockIndex = bucketOffset >>> this.blockSizeBits;
			--usedBlockNum;
			// push the block to the back
			freeBlockIndexes[freeBlockNum++] = blockIndex;
		}

		/**
		 * Returns true if this bucket has free blocks to allocate.
		 */
		boolean hasFreeBlocks() {
			return freeBlockNum > 0;
		}

		/**
		 * Returns true if there is no blocks used in this bucket.
		 */
		public boolean isFree() {
			return usedBlockNum == 0;
		}

		int getBlockAllocatorIndex() {
			return blockAllocatorIndex;
		}

		int getBlockSize() {
			return blockSize;
		}

		boolean isUninitiated() {
			return blockAllocatorIndex == -1;
		}

		int getFreeBlockNum() {
			return freeBlockNum;
		}

		int getUsedBlockNum() {
			return usedBlockNum;
		}

		int getFreeBytes() {
			return freeBlockNum << blockSizeBits;
		}

		int getUsedBytes() {
			return usedBlockNum << blockSizeBits;
		}

		long getBaseOffset() {
			return baseOffset;
		}
	}

	/**
	 * Allocates blocks with fixed size from buckets.
	 */
	static final class BlockAllocator {

		/**
		 * Index of this block allocator.
		 */
		private final int blockAllocatorIndex;

		/**
		 * Queue of free buckets. Allocator can apply a free bucket
		 * from the queue, and returns a free bucket to the queue.
		 */
		private final ConcurrentLinkedQueue<Bucket> freeBucketsQueue;

		/**
		 * Map of buckets applied by this allocator from queue.
		 */
		private LinkedMap appliedBuckets;

		/**
		 * Map of buckets that have free blocks to allocate.
		 */
		private LinkedMap freeBuckets;

		BlockAllocator(int blockAllocatorIndex, ConcurrentLinkedQueue<Bucket> freeBucketsQueue) {
			this.blockAllocatorIndex = blockAllocatorIndex;
			this.freeBucketsQueue = freeBucketsQueue;
			this.appliedBuckets = new LinkedMap();
			this.freeBuckets = new LinkedMap();
		}

		/**
		 * Allocates a block.
		 *
		 * @return offset of the block.
		 */
		synchronized int allocateBlock() {
			Bucket bucket = null;

			// 1. try to find a bucket that have free blocks
			if (freeBuckets.size() > 0) {
				bucket = (Bucket) freeBuckets.lastKey();
			}

			// 2. try to apply a new bucket from queue
			if (bucket == null) {
				bucket = freeBucketsQueue.poll();
				if (bucket != null) {
					addBucket(bucket);
				}
			}

			if (bucket == null) {
				return NO_SPACE;
			}

			int offset = bucket.allocate();
			if (!bucket.hasFreeBlocks()) {
				freeBuckets.remove(bucket);
			}

			return offset;
		}

		/**
		 * Frees a block from the given bucket.
		 *
		 * @param bucket bucket that block belongs to.
		 * @param offset offset of block to free.
		 */
		synchronized void freeBlock(Bucket bucket, int offset) {
			bucket.free(offset);
			if (bucket.isFree()) {
				removeBucket(bucket);
				freeBucketsQueue.offer(bucket);
			} else if (!freeBuckets.containsKey(bucket)) {
				freeBuckets.put(bucket, bucket);
			}
		}

		private void addBucket(Bucket b) {
			b.reuseBucket(blockAllocatorIndex);
			appliedBuckets.put(b, b);
			freeBuckets.put(b, b);
		}

		private void removeBucket(Bucket b) {
			appliedBuckets.remove(b);
			freeBuckets.remove(b);
		}

		@VisibleForTesting
		int getBlockAllocatorIndex() {
			return blockAllocatorIndex;
		}

		@VisibleForTesting
		LinkedMap getAppliedBuckets() {
			return appliedBuckets;
		}

		@VisibleForTesting
		LinkedMap getFreeBuckets() {
			return freeBuckets;
		}
	}
}
