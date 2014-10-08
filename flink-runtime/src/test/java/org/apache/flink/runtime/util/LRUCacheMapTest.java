/**
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

package org.apache.flink.runtime.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class LRUCacheMapTest {

	@Test
	public void testGetAndRemoveLRUWhenEmpty() {
		final LRUCacheMap<Integer, Integer> lruCache = new LRUCacheMap<Integer, Integer>();

		Assert.assertNull(lruCache.get(1));

		Assert.assertNull(lruCache.removeLRU());

		Assert.assertEquals(0, lruCache.size());
	}

	@Test
	public void testPutGetLRURemove() {
		final LRUCacheMap<Integer, Integer> lruCache = new LRUCacheMap<Integer, Integer>();

		lruCache.put(0, 0);
		lruCache.put(1, 1);
		lruCache.put(2, 2);
		lruCache.put(3, 3);
		lruCache.put(4, 4); // LRU -> 0, 1, 2, 3, 4

		Assert.assertEquals(0, lruCache.getLRU().intValue()); // 1, 2, 3, 4, 0

		Assert.assertEquals(1, lruCache.getLRU().intValue()); // 2, 3, 4, 0, 1

		lruCache.put(5, 5); // 2, 3, 4, 0, 1, 5

		Assert.assertEquals(2, lruCache.getLRU().intValue()); // 2, 3, 4, 0, 1, 5
		Assert.assertEquals(3, lruCache.getLRU().intValue()); // 3, 4, 0, 1, 5, 2
		Assert.assertEquals(4, lruCache.getLRU().intValue()); // 4, 0, 1, 5, 2, 3
		Assert.assertEquals(0, lruCache.getLRU().intValue()); // 0, 1, 5, 2, 3, 4
		Assert.assertEquals(1, lruCache.getLRU().intValue()); // 1, 5, 2, 3, 4, 0
		Assert.assertEquals(5, lruCache.getLRU().intValue()); // 5, 2, 3, 4, 0, 1
		Assert.assertEquals(2, lruCache.getLRU().intValue()); // 2, 3, 4, 0, 1, 5

		Assert.assertEquals(2, lruCache.remove(2).intValue()); // 3, 4, 0, 1, 5
		Assert.assertEquals(3, lruCache.removeLRU().intValue()); // 4, 0, 1, 5
		Assert.assertEquals(4, lruCache.removeLRU().intValue()); // 0, 1, 5
		Assert.assertEquals(0, lruCache.removeLRU().intValue()); // 1, 5
		Assert.assertEquals(1, lruCache.removeLRU().intValue()); // 5
		Assert.assertEquals(5, lruCache.removeLRU().intValue());

		Assert.assertTrue(lruCache.isEmpty());
	}

	@Test
	public void testPutGetRemoveLRU() {
		final LRUCacheMap<Integer, Integer> lruCache = new LRUCacheMap<Integer, Integer>();

		lruCache.put(0, 0);
		lruCache.put(1, 1);
		lruCache.put(2, 2);
		lruCache.put(3, 3);
		lruCache.put(4, 4); // LRU -> 0, 1, 2, 3, 4

		lruCache.get(1); // 0, 2, 3, 4, 1

		Assert.assertEquals(0, lruCache.removeLRU().intValue()); // 2, 3, 4, 1

		lruCache.get(2); // 3, 4, 1, 2

		lruCache.put(5, 5); // 3, 4, 1, 2, 5

		Assert.assertEquals(3, lruCache.removeLRU().intValue());
		Assert.assertEquals(4, lruCache.removeLRU().intValue());
		Assert.assertEquals(1, lruCache.removeLRU().intValue());
		Assert.assertEquals(2, lruCache.removeLRU().intValue());
		Assert.assertEquals(5, lruCache.removeLRU().intValue());
	}

	@Test
	public void testPutAndRemoveLRU() {
		final LRUCacheMap<Integer, Integer> lruCache = new LRUCacheMap<Integer, Integer>();

		final int numEntries = 100;

		// --------------------------------------------------------------------

		for (int i = 0; i < numEntries; i++) {
			// 1. Add random entries to the cache,
			lruCache.put(i, i);
		}

		for (int i = 0; i < numEntries; i++) {
			// 2. remove the least recently used element
			int lru = lruCache.removeLRU();

			// 3. and verify that it's in insertion order.
			Assert.assertEquals(i, lru);
		}

		// Verify that the cache is empty after all entries have been removed
		Assert.assertEquals(0, lruCache.size());
		Assert.assertNull(lruCache.removeLRU());
	}

	@Test
	public void testPutRandomGetRemoveLRU() {
		final LRUCacheMap<Integer, Integer> lruCache = new LRUCacheMap<Integer, Integer>();

		final Random random = new Random();

		final int numEntries = 5;

		final int[] expectedLruOrder = new int[numEntries];

		// --------------------------------------------------------------------

		for (int i = 0; i < numEntries; i++) {
			// 1. Add ascending numbers to the cache,
			lruCache.put(i, i);

			// Keeps track of the expected LRU access order for the element
			// with key i (the array index). Initially, LRU order is same as
			// the insertion order.
			expectedLruOrder[i] = i;
		}

		for (int i = 0; i < numEntries * 10; i++) {
			final int randomKey = random.nextInt(numEntries);

			int currentPosition = expectedLruOrder[randomKey];

			for (int j = 0; j < numEntries; j++) {
				if (expectedLruOrder[j] > currentPosition) {
					expectedLruOrder[j]--;
				}
			}

			expectedLruOrder[randomKey] = numEntries - 1;

			// 2. access random entries,
			lruCache.get(randomKey);
		}

		for (int i = 0; i < numEntries; i++) {
			// 3. remove the least recently used element,
			int lru = lruCache.removeLRU();

			// 4. and verify that it's in LRU access order.
			Assert.assertEquals(expectedLruOrder[lru], i);
		}

		// Verify that the cache is empty after all entries have been removed
		Assert.assertEquals(0, lruCache.size());
		Assert.assertNull(lruCache.removeLRU());
	}
}
