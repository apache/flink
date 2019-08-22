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

import java.util.Map;
import java.util.SortedMap;

import static org.apache.flink.runtime.state.heap.space.SpaceConstants.NO_SPACE;

/**
 * Tests for {@link DirectBucketAllocator}.
 */
public class DirectBucketAllocatorTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testNormal() {
		int capacity = 100;
		DirectBucketAllocator bucketAllocator = new DirectBucketAllocator(capacity);
		SortedMap<Integer, Integer> usedSpaces = bucketAllocator.getUsedSpace();
		SortedMap<Integer, Integer> freeSpaces = bucketAllocator.getFreeSpaces();
		int expectedUsed = 0;
		int expectedFree = 0;
		int expectedLeft = capacity;

		Assert.assertEquals(0, usedSpaces.size());
		Assert.assertEquals(0, freeSpaces.size());
		Assert.assertEquals(100, bucketAllocator.getCapacity());
		Assert.assertEquals(0, bucketAllocator.getOffset());
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// allocate a space with size 11
		int offset1 = bucketAllocator.allocate(11);
		Assert.assertEquals(0, offset1);
		Assert.assertEquals(11, usedSpaces.get(0).intValue());
		Assert.assertEquals(1, usedSpaces.size());
		Assert.assertEquals(0, freeSpaces.size());
		Assert.assertEquals(11, bucketAllocator.getOffset());
		expectedUsed += 11;
		expectedLeft -= 11;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// allocate a space with size 12
		int offset2 = bucketAllocator.allocate(12);
		Assert.assertEquals(11, offset2);
		Assert.assertEquals(11, usedSpaces.get(0).intValue());
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(2, usedSpaces.size());
		Assert.assertEquals(0, freeSpaces.size());
		Assert.assertEquals(23, bucketAllocator.getOffset());
		expectedUsed += 12;
		expectedLeft -= 12;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		//allocate a space with size 11
		int offset3 = bucketAllocator.allocate(11);
		Assert.assertEquals(23, offset3);
		Assert.assertEquals(11, usedSpaces.get(0).intValue());
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(3, usedSpaces.size());
		Assert.assertEquals(0, freeSpaces.size());
		Assert.assertEquals(34, bucketAllocator.getOffset());
		expectedUsed += 11;
		expectedLeft -= 11;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// free offset1
		bucketAllocator.free(offset1);
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(11, freeSpaces.get(0).intValue());
		Assert.assertEquals(2, usedSpaces.size());
		Assert.assertEquals(1, freeSpaces.size());
		Assert.assertEquals(34, bucketAllocator.getOffset());
		expectedUsed -= 11;
		expectedFree += 11;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);
		verifyAllocatorSpace(bucketAllocator, 23, 11, 66);

		// allocate a space with size 13
		int offset4 = bucketAllocator.allocate(13);
		Assert.assertEquals(34, offset4);
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(13, usedSpaces.get(34).intValue());
		Assert.assertEquals(11, freeSpaces.get(0).intValue());
		Assert.assertEquals(3, usedSpaces.size());
		Assert.assertEquals(1, freeSpaces.size());
		Assert.assertEquals(47, bucketAllocator.getOffset());
		expectedUsed += 13;
		expectedLeft -= 13;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// allocate a space with size 11 again, will reuse the free space which have the same size
		offset1 = bucketAllocator.allocate(11);
		Assert.assertEquals(0, offset1);
		Assert.assertEquals(11, usedSpaces.get(0).intValue());
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(13, usedSpaces.get(34).intValue());
		Assert.assertEquals(4, usedSpaces.size());
		Assert.assertEquals(0, freeSpaces.size());
		Assert.assertEquals(47, bucketAllocator.getOffset());
		expectedUsed += 11;
		expectedFree -= 11;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// allocate a space with size 14
		int offset5 = bucketAllocator.allocate(14);
		Assert.assertEquals(47, offset5);
		Assert.assertEquals(11, usedSpaces.get(0).intValue());
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(13, usedSpaces.get(34).intValue());
		Assert.assertEquals(14, usedSpaces.get(47).intValue());
		Assert.assertEquals(5, usedSpaces.size());
		Assert.assertEquals(0, freeSpaces.size());
		Assert.assertEquals(61, bucketAllocator.getOffset());
		expectedUsed += 14;
		expectedLeft -= 14;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// allocate a space with size 14
		int offset6 = bucketAllocator.allocate(15);
		Assert.assertEquals(61, offset6);
		Assert.assertEquals(11, usedSpaces.get(0).intValue());
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(13, usedSpaces.get(34).intValue());
		Assert.assertEquals(14, usedSpaces.get(47).intValue());
		Assert.assertEquals(15, usedSpaces.get(61).intValue());
		Assert.assertEquals(6, usedSpaces.size());
		Assert.assertEquals(0, freeSpaces.size());
		Assert.assertEquals(76, bucketAllocator.getOffset());
		expectedUsed += 15;
		expectedLeft -= 15;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// allocate a space with size 16
		int offset7 = bucketAllocator.allocate(16);
		Assert.assertEquals(76, offset7);
		Assert.assertEquals(11, usedSpaces.get(0).intValue());
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(13, usedSpaces.get(34).intValue());
		Assert.assertEquals(14, usedSpaces.get(47).intValue());
		Assert.assertEquals(15, usedSpaces.get(61).intValue());
		Assert.assertEquals(16, usedSpaces.get(76).intValue());
		Assert.assertEquals(7, usedSpaces.size());
		Assert.assertEquals(0, freeSpaces.size());
		Assert.assertEquals(92, bucketAllocator.getOffset());
		expectedUsed += 16;
		expectedLeft -= 16;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// there is no space to allocate
		int offset8 = bucketAllocator.allocate(9);
		Assert.assertEquals(NO_SPACE, offset8);
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		bucketAllocator.free(offset1);
		bucketAllocator.free(offset4);
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(14, usedSpaces.get(47).intValue());
		Assert.assertEquals(15, usedSpaces.get(61).intValue());
		Assert.assertEquals(16, usedSpaces.get(76).intValue());
		Assert.assertEquals(11, freeSpaces.get(0).intValue());
		Assert.assertEquals(13, freeSpaces.get(34).intValue());
		Assert.assertEquals(5, usedSpaces.size());
		Assert.assertEquals(2, freeSpaces.size());
		Assert.assertEquals(92, bucketAllocator.getOffset());
		expectedUsed -= 24;
		expectedFree += 24;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// allocate 12, close use offset 34.
		int offset9 = bucketAllocator.allocate(12);
		Assert.assertEquals(34, offset9);
		Assert.assertEquals(12, usedSpaces.get(11).intValue());
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(12, usedSpaces.get(34).intValue());
		Assert.assertEquals(14, usedSpaces.get(47).intValue());
		Assert.assertEquals(15, usedSpaces.get(61).intValue());
		Assert.assertEquals(16, usedSpaces.get(76).intValue());
		Assert.assertEquals(11, freeSpaces.get(0).intValue());
		Assert.assertEquals(1, freeSpaces.get(46).intValue());
		Assert.assertEquals(6, usedSpaces.size());
		Assert.assertEquals(2, freeSpaces.size());
		expectedUsed += 12;
		expectedFree -= 12;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		bucketAllocator.free(11);
		bucketAllocator.free(47);
		bucketAllocator.free(61);
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(12, usedSpaces.get(34).intValue());
		Assert.assertEquals(16, usedSpaces.get(76).intValue());
		Assert.assertEquals(11, freeSpaces.get(0).intValue());
		Assert.assertEquals(12, freeSpaces.get(11).intValue());
		Assert.assertEquals(1, freeSpaces.get(46).intValue());
		Assert.assertEquals(14, freeSpaces.get(47).intValue());
		Assert.assertEquals(15, freeSpaces.get(61).intValue());
		Assert.assertEquals(3, usedSpaces.size());
		Assert.assertEquals(5, freeSpaces.size());
		expectedUsed -= 41;
		expectedFree += 41;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// compaction
		Assert.assertEquals(NO_SPACE, bucketAllocator.allocate(31));
		Assert.assertEquals(11, usedSpaces.get(23).intValue());
		Assert.assertEquals(12, usedSpaces.get(34).intValue());
		Assert.assertEquals(16, usedSpaces.get(76).intValue());
		Assert.assertEquals(23, freeSpaces.get(0).intValue());
		Assert.assertEquals(30, freeSpaces.get(46).intValue());

		bucketAllocator.free(23);
		bucketAllocator.free(34);
		bucketAllocator.free(76);
		expectedUsed -= 39;
		expectedFree += 39;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);

		// compaction
		Assert.assertEquals(0, bucketAllocator.allocate(24));
		Assert.assertEquals(24, usedSpaces.get(0).intValue());
		Assert.assertEquals(68, freeSpaces.get(24).intValue());
		Assert.assertEquals(92, bucketAllocator.getOffset());
		expectedUsed += 24;
		expectedFree -= 24;
		verifyAllocatorSpace(bucketAllocator, expectedUsed, expectedFree, expectedLeft);
	}

	private void verifyAllocatorSpace(
		DirectBucketAllocator bucketAllocator,
		int expectedUsed,
		int expectedFree,
		int expectedLeft) {
		int used = bucketAllocator.getUsedSpace().entrySet().stream().map(Map.Entry::getValue).reduce(0, (x, y) -> x + y);
		int free = bucketAllocator.getFreeSpaces().entrySet().stream().map(Map.Entry::getValue).reduce(0, (x, y) -> x + y);
		int left = bucketAllocator.getCapacity() - bucketAllocator.getOffset();
		Assert.assertEquals(expectedUsed, used);
		Assert.assertEquals(expectedFree, free);
		Assert.assertEquals(expectedLeft, left);
	}
}
