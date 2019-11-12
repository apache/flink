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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.state.heap.space.PowerTwoBucketAllocator.getBlockSizeFromBlockAllocatorIndex;
import static org.apache.flink.runtime.state.heap.space.SpaceConstants.NO_SPACE;

/**
 * Tests for {@link PowerTwoBucketAllocator}.
 */
public class PowerTwoBucketAllocatorTest {

	/**
	 * Tests that {@link PowerTwoBucketAllocator} can be constructed correctly.
	 */
	@Test
	public void testConstruct() {
		int chunkSize = 1024 * 1024 * 1024;
		int bucketSize = 1024 * 1024;
		PowerTwoBucketAllocator bucketAllocator = new PowerTwoBucketAllocator(chunkSize, bucketSize);

		Bucket[] allBuckets = bucketAllocator.getAllBuckets();
		BlockAllocator[] blockAllocators = bucketAllocator.getBlockAllocators();

		Assert.assertEquals(1024, allBuckets.length);
		Assert.assertEquals(16, blockAllocators.length);

		int bucketNum = 0;
		for (int i = 0; i < blockAllocators.length; i++) {
			Assert.assertEquals(i, blockAllocators[i].getBlockAllocatorIndex());
			for (Object object : blockAllocators[i].getAppliedBuckets().keySet()) {
				Bucket bucket = (Bucket) object;
				Assert.assertEquals(i, bucket.getBlockAllocatorIndex());
				Assert.assertTrue(bucket.isFree());
				Assert.assertTrue(!bucket.isUninitiated());
				Assert.assertEquals(1 << (i + 5), bucket.getBlockSize());
				Assert.assertEquals(1024 * 1024 / (1 << (i + 5)), bucket.getFreeBlockNum());
				Assert.assertEquals(0, bucket.getUsedBlockNum());
				Assert.assertEquals(1024 * 1024, bucket.getFreeBytes());
				Assert.assertEquals(0, bucket.getUsedBytes());
				bucketNum++;
			}
		}
		Assert.assertEquals(bucketNum, 0);
		Assert.assertEquals(1024, bucketAllocator.getFreeBucketsQueue().size());
	}

	/**
	 * Tests that {@link Bucket} works well.
	 */
	@Test
	public void testBucket() {
		doTestBucket(0, 1024);
		doTestBucket(32, 1024);
		doTestBucket(64, 1024);
		doTestBucket(128, 1024);
		doTestBucket(256, 1024);
		doTestBucket(512, 1024);
		doTestBucket(1024, 1024);
		doTestBucket(1024, 1024 * 1024);
	}

	private void doTestBucket(int baseOffset, int bucketSize) {
		Bucket bucket = new Bucket(baseOffset, bucketSize);

		// test bucket is uninitiated
		Assert.assertTrue(bucket.isUninitiated());
		Assert.assertEquals(baseOffset, bucket.getBaseOffset());
		Assert.assertEquals(0, bucket.getUsedBlockNum());
		Assert.assertEquals(0, bucket.getFreeBlockNum());
		Assert.assertEquals(0, bucket.getUsedBytes());
		Assert.assertEquals(0, bucket.getFreeBytes());

		// instantiate 32bytes
		int bucketAllocatorIndex = 2;
		int blockSize = 1 << (bucketAllocatorIndex + 5);
		bucket.reuseBucket(bucketAllocatorIndex);
		Assert.assertEquals(baseOffset, bucket.getBaseOffset());
		Assert.assertEquals(0, bucket.getUsedBlockNum());
		Assert.assertEquals(bucketSize / blockSize, bucket.getFreeBlockNum());
		Assert.assertEquals(0, bucket.getUsedBytes());
		Assert.assertEquals(blockSize, bucket.getBlockSize());
		Assert.assertEquals(bucketSize, bucket.getFreeBytes());
		Assert.assertTrue(bucket.isFree());
		Assert.assertTrue(bucket.hasFreeBlocks());

		int offset = bucket.allocate();
		Assert.assertEquals(baseOffset + bucketSize - blockSize, offset);
		Assert.assertEquals(1, bucket.getUsedBlockNum());
		Assert.assertEquals(bucketSize / blockSize - 1, bucket.getFreeBlockNum());

		bucket.free(offset);
		Assert.assertEquals(0, bucket.getUsedBlockNum());
		Assert.assertEquals(bucketSize / blockSize, bucket.getFreeBlockNum());

		List<Integer> allOffset = new ArrayList<>();
		while (bucket.hasFreeBlocks()) {
			offset = bucket.allocate();
			allOffset.add(offset);
			int totalAllocate = allOffset.size();
			Assert.assertEquals(baseOffset + bucketSize - totalAllocate * blockSize, offset);
			Assert.assertEquals(totalAllocate, bucket.getUsedBlockNum());
			Assert.assertEquals(bucketSize / blockSize - totalAllocate, bucket.getFreeBlockNum());
		}

		Assert.assertEquals(bucketSize / blockSize, allOffset.size());
		Assert.assertEquals(bucketSize / blockSize, (new HashSet<>(allOffset)).size());

		Collections.shuffle(allOffset);
		// disorder freed offsets
		for (Integer off : allOffset) {
			bucket.free(off);
		}

		allOffset = new ArrayList<>();
		while (bucket.hasFreeBlocks()) {
			offset = bucket.allocate();
			allOffset.add(offset);
			int totalAllocate = allOffset.size();
			Assert.assertEquals(totalAllocate, bucket.getUsedBlockNum());
			Assert.assertEquals(bucketSize / blockSize - totalAllocate, bucket.getFreeBlockNum());
		}

		Assert.assertEquals(bucketSize / blockSize, allOffset.size());
		Assert.assertEquals(bucketSize / blockSize, (new HashSet<>(allOffset)).size());
	}

	/**
	 * Tests conversion between block allocator index and block size.
	 */
	@Test
	public void testConversionBetweenBlockSizeAndBlockAllocatorIndex() {
		// index 0 indicates that block size is 32 (2 << 5), and index 1 indicates a 64 bytes block
		Assert.assertEquals(0, PowerTwoBucketAllocator.getBlockAllocatorIndex(1));
		Assert.assertEquals(0, PowerTwoBucketAllocator.getBlockAllocatorIndex(32));
		Assert.assertEquals(1, PowerTwoBucketAllocator.getBlockAllocatorIndex(33));
		Assert.assertEquals(1, PowerTwoBucketAllocator.getBlockAllocatorIndex(64));
		Assert.assertEquals(2, PowerTwoBucketAllocator.getBlockAllocatorIndex(65));
		Assert.assertEquals(2, PowerTwoBucketAllocator.getBlockAllocatorIndex(128));
		Assert.assertEquals(3, PowerTwoBucketAllocator.getBlockAllocatorIndex(129));
		Assert.assertEquals(3, PowerTwoBucketAllocator.getBlockAllocatorIndex(256));
		Assert.assertEquals(16, PowerTwoBucketAllocator.getBlockAllocatorIndex(2 * 1024 * 1024));
		Assert.assertEquals(17, PowerTwoBucketAllocator.getBlockAllocatorIndex(2 * 1024 * 1024 + 1));
		Assert.assertEquals(17, PowerTwoBucketAllocator.getBlockAllocatorIndex(4 * 1024 * 1024));

		Assert.assertEquals(32, getBlockSizeFromBlockAllocatorIndex(0));
		Assert.assertEquals(64, getBlockSizeFromBlockAllocatorIndex(1));
		Assert.assertEquals(128, getBlockSizeFromBlockAllocatorIndex(2));
		Assert.assertEquals(4194304, getBlockSizeFromBlockAllocatorIndex(17));
	}

	/**
	 * Tests that {@link BlockAllocator} works well.
	 */
	@Test
	public void testBlockAllocator() {
		PowerTwoBucketAllocator bucketAllocator = new PowerTwoBucketAllocator(1024, 128);

		Bucket[] allBuckets = bucketAllocator.getAllBuckets();
		BlockAllocator[] blockAllocators = bucketAllocator.getBlockAllocators();

		Assert.assertEquals(8, allBuckets.length);
		Assert.assertEquals(3, blockAllocators.length);

		Assert.assertEquals(0, blockAllocators[0].getAppliedBuckets().size());
		Assert.assertEquals(0, blockAllocators[0].getFreeBuckets().size());
		Assert.assertEquals(8, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, blockAllocators[1].getAppliedBuckets().size());
		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(8, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, blockAllocators[2].getAppliedBuckets().size());
		Assert.assertEquals(0, blockAllocators[2].getFreeBuckets().size());
		Assert.assertEquals(8, bucketAllocator.getFreeBucketsQueue().size());

		for (int i = 0; i < 3; i++) {
			int bucketCount = blockAllocators[i].getAppliedBuckets().size();
			int blockSize = 1 << (i + 5);
			int blockNum = 128 / blockSize;
			for (Object object : blockAllocators[i].getAppliedBuckets().keySet()) {
				Assert.assertEquals(blockSize, ((Bucket) object).getBlockSize());
				Assert.assertEquals(blockNum, ((Bucket) object).getFreeBlockNum());
			}

			Assert.assertEquals(0, getUsedBlocks(blockAllocators[i]));
			Assert.assertEquals(bucketCount * blockNum, getFreeBlocks(blockAllocators[i]));
			Assert.assertEquals(blockSize, getBlockSize(blockAllocators[i]));
		}

		// allocate 128, use distributed bucket
		int offset1 = blockAllocators[2].allocateBlock();
		Assert.assertEquals(0, bucketAllocator.getBucketIndex(offset1));
		Assert.assertEquals(0, offset1);

		Assert.assertEquals(0, getFreeBlocks(blockAllocators[0]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[2]));
		Assert.assertEquals(1, getUsedBlocks(blockAllocators[2]));
		Assert.assertEquals(128, getUsedBytes(blockAllocators[2]));
		Assert.assertEquals(0, blockAllocators[2].getFreeBuckets().size());
		Assert.assertEquals(7, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(1, blockAllocators[2].getAppliedBuckets().size());

		// allocate 128 again, will ask info[0] to get bucket
		int offset2 = blockAllocators[2].allocateBlock();
		Assert.assertEquals(1, bucketAllocator.getBucketIndex(offset2));
		Assert.assertEquals(128, offset2);

		Assert.assertEquals(0, getFreeBlocks(blockAllocators[0]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[2]));
		Assert.assertEquals(2, getUsedBlocks(blockAllocators[2]));
		Assert.assertEquals(256, getUsedBytes(blockAllocators[2]));

		Assert.assertEquals(0, blockAllocators[2].getFreeBuckets().size());
		Assert.assertEquals(6, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(2, blockAllocators[2].getAppliedBuckets().size());

		Assert.assertEquals(0, blockAllocators[0].getFreeBuckets().size());
		Assert.assertEquals(6, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, blockAllocators[0].getAppliedBuckets().size());

		Assert.assertEquals(0, blockAllocators[1].getAppliedBuckets().size());
		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(6, bucketAllocator.getFreeBucketsQueue().size());

		// free 128, check bucket list
		bucketAllocator.free(offset2);

		Assert.assertEquals(0, getFreeBlocks(blockAllocators[0]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[2]));
		Assert.assertEquals(1, getUsedBlocks(blockAllocators[2]));
		Assert.assertEquals(128, getUsedBytes(blockAllocators[2]));

		Assert.assertEquals(0,
			((Bucket) blockAllocators[2].getAppliedBuckets().get(0)).getBaseOffset());

		Assert.assertEquals(0, blockAllocators[2].getFreeBuckets().size());
		Assert.assertEquals(7, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(1, blockAllocators[2].getAppliedBuckets().size());

		Assert.assertEquals(0, blockAllocators[0].getFreeBuckets().size());
		Assert.assertEquals(7, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, blockAllocators[0].getAppliedBuckets().size());

		Assert.assertEquals(0, blockAllocators[1].getAppliedBuckets().size());
		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(7, bucketAllocator.getFreeBucketsQueue().size());

		// allocate 64 check free and CompletelyFree
		int offset3 = blockAllocators[1].allocateBlock();
		Assert.assertEquals(2, bucketAllocator.getBucketIndex(offset3));
		Assert.assertEquals(320, offset3);

		Assert.assertEquals(1, blockAllocators[1].getAppliedBuckets().size());
		Assert.assertEquals(1, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(6, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(1, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(1, getUsedBlocks(blockAllocators[1]));
		Assert.assertEquals(64, getUsedBytes(blockAllocators[1]));

		// allocate 64 check free and CompletelyFree again
		int offset4 = blockAllocators[1].allocateBlock();
		Assert.assertEquals(2, bucketAllocator.getBucketIndex(offset4));
		Assert.assertEquals(256, offset4);

		Assert.assertEquals(1, blockAllocators[1].getAppliedBuckets().size());
		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(6, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(2, getUsedBlocks(blockAllocators[1]));
		Assert.assertEquals(128, getUsedBytes(blockAllocators[1]));

		// free 64, and check again
		bucketAllocator.free(offset3);
		Assert.assertEquals(1, blockAllocators[1].getAppliedBuckets().size());
		Assert.assertEquals(1, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(6, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(1, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(1, getUsedBlocks(blockAllocators[1]));
		Assert.assertEquals(64, getUsedBytes(blockAllocators[1]));

		// free 64 and all are free, and check again
		bucketAllocator.free(offset4);
		Assert.assertEquals(0, blockAllocators[1].getAppliedBuckets().size());
		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(7, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getUsedBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getUsedBytes(blockAllocators[1]));
	}

	@Test
	public void testNormal() {
		PowerTwoBucketAllocator bucketAllocator = new PowerTwoBucketAllocator(128, 64);

		Bucket[] allBuckets = bucketAllocator.getAllBuckets();
		BlockAllocator[] blockAllocators = bucketAllocator.getBlockAllocators();

		Assert.assertEquals(2, allBuckets.length);
		Assert.assertEquals(2, blockAllocators.length);

		Assert.assertEquals(0, blockAllocators[0].getFreeBuckets().size());
		Assert.assertEquals(2, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, getUsedBlocks(blockAllocators[0]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[0]));
		Assert.assertEquals(0, getUsedBytes(blockAllocators[0]));

		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(2, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, getUsedBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getUsedBytes(blockAllocators[1]));

		try {
			bucketAllocator.allocate(65);
			Assert.fail("Should throw exception");
		} catch (Exception e) {
			Assert.assertEquals("PowerTwoBucketAllocator can't allocate size larger than bucket size", e.getMessage());
		}

		try {
			bucketAllocator.allocate(0);
			Assert.fail("Should throw exception");
		} catch (Exception e) {
			Assert.assertTrue(e instanceof IllegalArgumentException);
			Assert.assertTrue(e.getMessage().contains("Size to allocate must be positive, but the actual is"));
		}

		int offset1 = bucketAllocator.allocate(1);
		Assert.assertEquals(32, offset1);

		Assert.assertEquals(1, blockAllocators[0].getFreeBuckets().size());
		Assert.assertEquals(1, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(1, getUsedBlocks(blockAllocators[0]));
		Assert.assertEquals(1, getFreeBlocks(blockAllocators[0]));
		Assert.assertEquals(32, getUsedBytes(blockAllocators[0]));

		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(1, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, getUsedBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getUsedBytes(blockAllocators[1]));

		int offset2 = bucketAllocator.allocate(2);
		Assert.assertEquals(0, offset2);

		Assert.assertEquals(0, blockAllocators[0].getFreeBuckets().size());
		Assert.assertEquals(1, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(2, getUsedBlocks(blockAllocators[0]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[0]));
		Assert.assertEquals(64, getUsedBytes(blockAllocators[0]));

		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(1, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, getUsedBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getUsedBytes(blockAllocators[1]));

		//allocate >32
		int offset3 = bucketAllocator.allocate(33);
		Assert.assertEquals(64, offset3);

		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(0, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(1, getUsedBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(64, getUsedBytes(blockAllocators[1]));

		Assert.assertEquals(NO_SPACE, bucketAllocator.allocate(32));

		bucketAllocator.free(0);
		bucketAllocator.free(32);
		bucketAllocator.free(64);

		Assert.assertEquals(0, blockAllocators[0].getFreeBuckets().size());
		Assert.assertEquals(2, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, getUsedBlocks(blockAllocators[0]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[0]));
		Assert.assertEquals(0, getUsedBytes(blockAllocators[0]));

		Assert.assertEquals(0, blockAllocators[1].getFreeBuckets().size());
		Assert.assertEquals(2, bucketAllocator.getFreeBucketsQueue().size());
		Assert.assertEquals(0, getUsedBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getFreeBlocks(blockAllocators[1]));
		Assert.assertEquals(0, getUsedBytes(blockAllocators[1]));
	}

	@Test
	public void testConcurrentAllocateAndFree() throws InterruptedException {
		PowerTwoBucketAllocator power = new PowerTwoBucketAllocator(1024 * 1024, 128);

		Bucket[] allBuckets = power.getAllBuckets();
		BlockAllocator[] bucketSizeInfo = power.getBlockAllocators();

		Assert.assertEquals(8192, allBuckets.length);
		Assert.assertEquals(3, bucketSizeInfo.length);
		Assert.assertEquals(32, getBlockSizeFromBlockAllocatorIndex(bucketSizeInfo[0].getBlockAllocatorIndex()));
		Assert.assertEquals(64, getBlockSizeFromBlockAllocatorIndex(bucketSizeInfo[1].getBlockAllocatorIndex()));
		Assert.assertEquals(128, getBlockSizeFromBlockAllocatorIndex(bucketSizeInfo[2].getBlockAllocatorIndex()));

		int threadNum = 10;
		Thread[] threads = new Thread[threadNum];
		AtomicBoolean hasException = new AtomicBoolean(false);
		for (int i = 0; i < threadNum; i++) {
			threads[i] = new Thread(() -> {
				int i1 = 0;
				while (i1++ < 100000) {
					try {
						int offset = power.allocate(45);
						if (offset != NO_SPACE) {
							power.free(offset);
						}
					} catch (Throwable e) {
						e.printStackTrace();
						hasException.set(true);
					}
				}
			});
		}

		for (Thread t : threads) {
			t.start();
		}
		for (Thread t : threads) {
			t.join();
		}
		Assert.assertFalse(hasException.get());
	}

	@SuppressWarnings("unchecked")
	private int getFreeBlocks(BlockAllocator blockAllocator) {
		int sum = 0;
		for (Object obj : blockAllocator.getAppliedBuckets().keySet()) {
			sum += ((Bucket) obj).getFreeBlockNum();
		}
		return sum;
	}

	@SuppressWarnings("unchecked")
	private int getUsedBlocks(BlockAllocator blockAllocator) {
		int sum = 0;
		for (Object obj : blockAllocator.getAppliedBuckets().keySet()) {
			sum += ((Bucket) obj).getUsedBlockNum();
		}
		return sum;
	}

	@SuppressWarnings("unchecked")
	private int getUsedBytes(BlockAllocator blockAllocator) {
		int sum = 0;
		for (Object obj : blockAllocator.getAppliedBuckets().keySet()) {
			sum += ((Bucket) obj).getUsedBytes();
		}
		return sum;
	}

	private int getBlockSize(BlockAllocator blockAllocator) {
		return getBlockSizeFromBlockAllocatorIndex(blockAllocator.getBlockAllocatorIndex());
	}
}
