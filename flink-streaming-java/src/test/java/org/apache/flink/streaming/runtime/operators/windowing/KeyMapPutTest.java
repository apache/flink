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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.junit.Test;

import java.util.BitSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link KeyMap}.
 */
public class KeyMapPutTest {

	@Test
	public void testPutUniqueKeysAndGrowth() {
		try {
			KeyMap<Integer, Integer> map = new KeyMap<>();

			final int numElements = 1000000;

			for (int i = 0; i < numElements; i++) {
				map.put(i, 2 * i + 1);

				assertEquals(i + 1, map.size());
				assertTrue(map.getCurrentTableCapacity() > map.size());
				assertTrue(map.getCurrentTableCapacity() > map.getRehashThreshold());
				assertTrue(map.size() <= map.getRehashThreshold());
			}

			assertEquals(numElements, map.size());
			assertEquals(numElements, map.traverseAndCountElements());
			assertEquals(1 << 21, map.getCurrentTableCapacity());

			for (int i = 0; i < numElements; i++) {
				assertEquals(2 * i + 1, map.get(i).intValue());
			}

			for (int i = numElements - 1; i >= 0; i--) {
				assertEquals(2 * i + 1, map.get(i).intValue());
			}

			BitSet bitset = new BitSet();
			int numContained = 0;
			for (KeyMap.Entry<Integer, Integer> entry : map) {
				numContained++;

				assertEquals(entry.getKey() * 2 + 1, entry.getValue().intValue());
				assertFalse(bitset.get(entry.getKey()));
				bitset.set(entry.getKey());
			}

			assertEquals(numElements, numContained);
			assertEquals(numElements, bitset.cardinality());

			assertEquals(numElements, map.size());
			assertEquals(numElements, map.traverseAndCountElements());
			assertEquals(1 << 21, map.getCurrentTableCapacity());
			assertTrue(map.getLongestChainLength() <= 7);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPutDuplicateKeysAndGrowth() {
		try {
			final KeyMap<Integer, Integer> map = new KeyMap<>();
			final int numElements = 1000000;

			for (int i = 0; i < numElements; i++) {
				Integer put = map.put(i, 2 * i + 1);
				assertNull(put);
			}

			for (int i = 0; i < numElements; i += 3) {
				Integer put = map.put(i, 2 * i);
				assertNotNull(put);
				assertEquals(2 * i + 1, put.intValue());
			}

			for (int i = 0; i < numElements; i++) {
				int expected = (i % 3 == 0) ? (2 * i) : (2 * i + 1);
				assertEquals(expected, map.get(i).intValue());
			}

			assertEquals(numElements, map.size());
			assertEquals(numElements, map.traverseAndCountElements());
			assertEquals(1 << 21, map.getCurrentTableCapacity());
			assertTrue(map.getLongestChainLength() <= 7);

			BitSet bitset = new BitSet();
			int numContained = 0;
			for (KeyMap.Entry<Integer, Integer> entry : map) {
				numContained++;

				int key = entry.getKey();
				int expected = key % 3 == 0 ? (2 * key) : (2 * key + 1);

				assertEquals(expected, entry.getValue().intValue());
				assertFalse(bitset.get(key));
				bitset.set(key);
			}

			assertEquals(numElements, numContained);
			assertEquals(numElements, bitset.cardinality());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
