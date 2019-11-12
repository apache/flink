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

import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

/**
 * A bucket is a continuous space that is divided into blocks with fixed size.
 * Buckets can be reused to allocate another size of blocks after it's freed.
 */
public final class Bucket {

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
		this.blockSize = PowerTwoBucketAllocator.getBlockSizeFromBlockAllocatorIndex(this.blockAllocatorIndex);
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
