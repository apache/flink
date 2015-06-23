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

public class DiscreteHistogramTest {

	private static HashMap<Double, Integer> entrySet;

	@Test(expected = IllegalArgumentException.class)
	public void testFailNegativeCounters() {
		DiscreteHistogram histogram = new DiscreteHistogram();
		entrySet = new HashMap<>();
		entrySet.put(2.0, 3);
		entrySet.put(1.0, 2);
		entrySet.put(3.0, -1);
		histogram.fill(entrySet.entrySet());
	}

	@Test
	public void testAdd() {
		DiscreteHistogram h = new DiscreteHistogram();
		h.add(1.0);
		h.add(2.0);
		h.add(3.0);
		h.add(1.0);
		h.add(3.0);
		h.add(3.0);
		h.add(4.0);
		assertEquals(2, h.count(1.0));
		assertEquals(1, h.count(2.0));
		assertEquals(3, h.count(3.0));
		assertEquals(1, h.count(4.0));
		assertEquals(0, h.count(5.0));
		assertEquals(7, h.getTotal());
		assertEquals(4, h.getSize());
	}

	@Test
	public void testMerge() {
		DiscreteHistogram h1 = new DiscreteHistogram();
		h1.add(1.0);
		h1.add(2.0);
		h1.add(3.0);
		h1.add(1.0);
		h1.add(3.0);
		h1.add(3.0);
		h1.add(4.0);
		DiscreteHistogram h2 = new DiscreteHistogram();
		entrySet = new HashMap<>();
		entrySet.put(2.0, 3);
		entrySet.put(1.0, 2);
		entrySet.put(3.0, 1);
		h2.fill(entrySet.entrySet());
		h1.merge(h2);
		assertEquals(4, h1.count(1.0));
		assertEquals(4, h1.count(2.0));
		assertEquals(4, h1.count(3.0));
		assertEquals(1, h1.count(4.0));
		assertEquals(13, h1.getTotal());
		assertEquals(4, h1.getSize());
	}

	@Test
	public void testEntropyAndGini() {
		DiscreteHistogram h = new DiscreteHistogram();
		h.add(1.0);
		h.add(2.0);
		h.add(2.0);
		h.add(3.0);

		assertEquals(1.5, h.entropy(), 1e-6);
		assertEquals(5.0 / 8, h.gini(), 1e-6);
	}
}
