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

import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.annotation.VisibleForTesting;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.runtime.state.heap.space.SpaceConstants.NO_SPACE;

/**
 * Allocates blocks with fixed size from {@link Bucket}.
 */
final class BlockAllocator {

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
