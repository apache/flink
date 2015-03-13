/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.policy;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.streaming.api.windowing.policy.EvictionPolicy;
import org.junit.Test;

import static org.junit.Assert.*;

public class CountEvictionPolicyTest {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCountEvictionPolicy() {
		List<Integer> tuples = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		int counter;

		// The count policy should not care about the triggered parameter
		// Therefore its value switches after each use in this test.
		boolean triggered = false;
		// the size of the buffer should not matter as well!

		// Test count of different sizes (0..9)
		for (int i = 0; i < 10; i++) {
			EvictionPolicy evictionPolicy = new CountEvictionPolicy(i, i);
			counter = 0;

			// Test first i steps (should not evict)
			for (int j = 0; j < i; j++) {
				counter++;
				assertEquals("Evictionpolicy with count of " + i + " evicted tuples at add nr. "
						+ counter + ". It should not evict for the first " + i + " adds.", 0,
						evictionPolicy.notifyEviction(tuples.get(j), (triggered = !triggered),
								tuples.get(Math.abs((i - j)) % 10)));
			}

			// Test the next three evictions
			for (int j = 0; j < 3; j++) {
				// The first add should evict now
				counter++;
				assertEquals("Evictionpolicy with count of " + i
						+ " did not evict correct number of tuples at the expected pos " + counter
						+ ".", i, evictionPolicy.notifyEviction(tuples.get(j),
						(triggered = !triggered), tuples.get(Math.abs((i - j)) % 10)));

				// the next i-1 adds should not evict
				for (int k = 0; k < i - 1; k++) {
					counter++;
					assertEquals("Evictionpolicy with count of " + i
							+ " evicted tuples at add nr. " + counter, 0,
							evictionPolicy.notifyEviction(tuples.get(j), (triggered = !triggered),
									tuples.get(Math.abs((i - j)) % 10)));
				}
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCountEvictionPolicyStartValuesAndEvictionAmount() {

		// The count policy should not care about the triggered parameter
		// Therefore its value switches after each use in this test.
		boolean triggered = false;
		// the size of the buffer should not matter as well!

		List<Integer> tuples = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

		// Text different eviction amounts (0..3)
		for (int x = 0; x < 4; x++) {

			// Test count of different sizes (0..9)
			for (int i = 0; i < 10; i++) {

				int counter = 0;

				// Test different start values (-5..5)
				for (int j = -5; i < 6; i++) {
					EvictionPolicy evictionPolicy = new CountEvictionPolicy(i, x, j);
					// Add tuples without eviction
					for (int k = 0; k < ((i - j > 0) ? i - j : 0); k++) {
						counter++;
						assertEquals("Evictionpolicy with count of " + i
								+ " did not evict correct number of tuples at the expected pos "
								+ counter + ".", 0, evictionPolicy.notifyEviction(
								tuples.get(Math.abs(j)), (triggered = !triggered),
								tuples.get(Math.abs((i - j)) % 10)));
					}
					// Expect eviction
					counter++;
					assertEquals("Evictionpolicy with count of " + i
							+ " did not evict correct number of tuples at the expected pos "
							+ counter + ".", x, evictionPolicy.notifyEviction(
							tuples.get(Math.abs(j)), (triggered = !triggered),
							tuples.get(Math.abs((i - j)) % 10)));
				}
			}
		}
	}

	@Test
	public void equalityTest() {
		assertEquals(new CountEvictionPolicy<Integer>(5, 5, 5), new CountEvictionPolicy<Integer>(5,
				5, 5));

		assertEquals(new CountEvictionPolicy<Integer>(5, 5), new CountEvictionPolicy<Integer>(5, 5));
		assertEquals(new CountEvictionPolicy<Integer>(5), new CountEvictionPolicy<Integer>(5));

		assertNotEquals(new CountEvictionPolicy<Integer>(4, 5, 5),
				new CountEvictionPolicy<Integer>(5, 5, 5));
		assertNotEquals(new CountEvictionPolicy<Integer>(5, 5, 5),
				new CountEvictionPolicy<Integer>(5, 4, 5));

		assertNotEquals(new CountEvictionPolicy<Integer>(5, 5, 5),
				new CountEvictionPolicy<Integer>(5, 5, 4));
	}

}
