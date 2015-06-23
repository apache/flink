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

package org.apache.flink.api.common.accumulators;

import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class ContinuousHistogramTest {

	private static HashMap<Double, Integer> entrySet;

	@Test(expected = IllegalArgumentException.class)
	public void testFailInvalidBin() {
		new ContinuousHistogram(0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailNegativeCounters() {
		ContinuousHistogram histogram = new ContinuousHistogram(3);
		entrySet = new HashMap<>();
		entrySet.put(2.0, 3);
		entrySet.put(1.0, 2);
		entrySet.put(3.0, -1);
		histogram.fill(entrySet.entrySet());
	}

	@Test
	public void testAdd() {
		ContinuousHistogram h = new ContinuousHistogram(2);
		entrySet = new HashMap<>();
		entrySet.put(4.0, 2);
		entrySet.put(5.0, 6);
		h.fill(entrySet.entrySet());
		assertEquals(2, (int) h.treeMap.get(4.0));
		assertEquals(6, (int) h.treeMap.get(5.0));
		assertEquals(2, h.getSize());
		assertEquals(8, h.getTotal());
		assertEquals(4.0, h.min(), 0.0);
		assertEquals(5.0, h.max(), 0.0);
		assertEquals(4.75, h.mean(), 0.0);
		assertEquals(0.1875, h.variance(), 0.0);

		h = new ContinuousHistogram(3);
		entrySet = new HashMap<>();
		entrySet.put(1.0, 3);
		entrySet.put(3.0, 7);
		entrySet.put(7.0, 1);
		entrySet.put(10.0, 2);
		h.fill(entrySet.entrySet());
		assertEquals(10, (int) h.treeMap.get(2.4));
		assertEquals(1, (int) h.treeMap.get(7.0));
		assertEquals(2, (int) h.treeMap.get(10.0));
		assertEquals(3, h.getSize());
		assertEquals(13, h.getTotal());
		assertEquals(1.0, h.min(), 0.0);
		assertEquals(10.0, h.max(), 0.0);
		assertEquals(3.923, h.mean(), 1e-3);
		assertEquals(8.84, h.variance(), 1e-3);
		h.add(5.0);
		assertEquals(3, h.getSize());
		assertEquals(14, h.getTotal());
		assertEquals(10, (int) h.treeMap.get(2.4));
		assertEquals(2, (int) h.treeMap.get(6.0));
		assertEquals(2, (int) h.treeMap.get(10.0));
		assertEquals(1.0, h.min(), 0.0);
		assertEquals(10.0, h.max(), 0.0);
		assertEquals(4, h.mean(), 0.0);
		assertEquals(8.286, h.variance(), 1e-3);

		h = new ContinuousHistogram(2);
		h.add(1.0);
		h.add(5.0);
		h.add(7.0);
		h.add(6.0);
		h.add(1.0);
		assertEquals(2, h.getSize());
		assertEquals(2, (int) h.treeMap.get(1.0));
		assertEquals(3, (int) h.treeMap.get(6.0));
		assertEquals(5, h.getTotal());
		assertEquals(1.0, h.min(), 0.0);
		assertEquals(7.0, h.max(), 0.0);
		assertEquals(4, h.mean(), 0.0);
		assertEquals(6.4, h.variance(), 1e-3);
	}

	@Test
	public void testMerge() {
		ContinuousHistogram h1 = new ContinuousHistogram(3);
		entrySet = new HashMap<>();
		entrySet.put(1.0, 3);
		entrySet.put(3.0, 7);
		entrySet.put(7.0, 1);
		entrySet.put(10.0, 2);
		h1.fill(entrySet.entrySet());

		ContinuousHistogram h2 = new ContinuousHistogram(2);
		entrySet = new HashMap<>();
		entrySet.put(4.0, 2);
		entrySet.put(5.0, 6);
		h2.fill(entrySet.entrySet());

		h2.merge(h1, 3);
		assertEquals(10, (int) h2.treeMap.get(2.4));
		assertEquals(9, (int) h2.treeMap.get(5.0));
		assertEquals(2, (int) h2.treeMap.get(10.0));
		assertEquals(3, h2.getSize());
		assertEquals(21, h2.getTotal());
		assertEquals(1.0, h2.min(), 0.0);
		assertEquals(10.0, h2.max(), 0.0);
		assertEquals(4.238, h2.mean(), 1e-3);
		assertEquals(5.705, h2.variance(), 1e-3);
	}

	@Test
	public void testQuantilesAndCounts() {
		ContinuousHistogram h = new ContinuousHistogram(5);
		entrySet = new HashMap<>();
		entrySet.put(1.0, 5);
		entrySet.put(3.0, 4);
		entrySet.put(7.0, 3);
		entrySet.put(10.0, 2);
		entrySet.put(11.0, 1);
		h.fill(entrySet.entrySet());

		assertEquals(1, h.count(h.quantile(0.05)));
		assertEquals(6, h.count(h.quantile(0.4)));
		assertEquals(12, h.count(h.quantile(0.8)));
		assertEquals(14, h.count(h.quantile(0.95)));
	}
}
