/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state.heap;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.TreeSet;

/**
 * Test base for instances of
 * {@link org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet.OrderedSetCache}.
 */
public abstract class OrderedSetCacheTestBase {

	@Test
	public void testOrderedSetCacheContract() {
		final Random random = new Random(0x42);
		final int capacity = 5000;
		final int keySpaceUpperBound = 100 * capacity;
		final TreeSet<Integer> checkSet = new TreeSet<>(Integer::compareTo);
		final CachingInternalPriorityQueueSet.OrderedSetCache<Integer> testInstance = createInstance(capacity);

		Assert.assertTrue(testInstance.isEmpty());

		while (checkSet.size() < capacity) {
			Assert.assertEquals(checkSet.size() >= capacity, testInstance.isFull());
			if (!checkSet.isEmpty() && random.nextInt(10) == 0) {
				final int toDelete = pickContainedRandomElement(checkSet, random);
				Assert.assertTrue(checkSet.remove(toDelete));
				testInstance.remove(toDelete);
			} else {
				final int randomValue = random.nextInt(keySpaceUpperBound);
				checkSet.add(randomValue);
				testInstance.add(randomValue);
			}
			Assert.assertEquals(checkSet.isEmpty(), testInstance.isEmpty());

			Assert.assertEquals(checkSet.first(), testInstance.peekFirst());
			Assert.assertEquals(checkSet.last(), testInstance.peekLast());

			Assert.assertFalse(testInstance.isInLowerBound(checkSet.last()));
			Assert.assertTrue(testInstance.isInLowerBound(checkSet.last() - 1));
		}

		Assert.assertTrue(testInstance.isFull());
		Assert.assertFalse(testInstance.isInLowerBound(checkSet.last()));
		Assert.assertTrue(testInstance.isInLowerBound(checkSet.last() - 1));

		testInstance.remove(pickNotContainedRandomElement(checkSet, random, keySpaceUpperBound));
		Assert.assertTrue(testInstance.isFull());

		int containedKey = pickContainedRandomElement(checkSet, random);

		Assert.assertTrue(checkSet.remove(containedKey));
		testInstance.remove(containedKey);

		Assert.assertFalse(testInstance.isFull());

		for (int i = 0; i < capacity; ++i) {
			if (random.nextInt(1) == 0) {
				Assert.assertEquals(checkSet.pollFirst(), testInstance.removeFirst());
			} else {
				Assert.assertEquals(checkSet.pollLast(), testInstance.removeLast());
			}
		}

		Assert.assertFalse(testInstance.isFull());
		Assert.assertTrue(testInstance.isEmpty());
	}

	private int pickNotContainedRandomElement(TreeSet<Integer> checkSet, Random random, int upperBound) {
		int notContainedKey;
		do {
			notContainedKey = random.nextInt(upperBound);
		} while (checkSet.contains(notContainedKey));
		return notContainedKey;
	}

	private int pickContainedRandomElement(TreeSet<Integer> checkSet, Random random) {
		assert !checkSet.isEmpty();
		return checkSet.ceiling(1 + random.nextInt(checkSet.last()));
	}

	protected abstract CachingInternalPriorityQueueSet.OrderedSetCache<Integer> createInstance(int capacity);
}
