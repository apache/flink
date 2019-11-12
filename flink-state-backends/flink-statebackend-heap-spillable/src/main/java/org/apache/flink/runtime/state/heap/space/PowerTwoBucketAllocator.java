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

import java.util.concurrent.ConcurrentLinkedQueue;

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

}
