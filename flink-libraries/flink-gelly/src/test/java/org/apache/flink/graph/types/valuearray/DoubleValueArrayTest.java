/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.types.DoubleValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DoubleValueArray}.
 */
public class DoubleValueArrayTest {

	@Test
	public void testBoundedArray() {
		int count = DoubleValueArray.DEFAULT_CAPACITY_IN_BYTES / DoubleValueArray.ELEMENT_LENGTH_IN_BYTES;

		ValueArray<DoubleValue> lva = new DoubleValueArray(DoubleValueArray.DEFAULT_CAPACITY_IN_BYTES);

		// fill the array
		for (int i = 0; i < count; i++) {
			assertFalse(lva.isFull());
			assertEquals(i, lva.size());

			assertTrue(lva.add(new DoubleValue(i)));

			assertEquals(i + 1, lva.size());
		}

		// array is now full
		assertTrue(lva.isFull());
		assertEquals(count, lva.size());

		// verify the array values
		int idx = 0;
		for (DoubleValue lv : lva) {
			assertEquals(idx++, lv.getValue(), 0.000001);
		}

		// add element past end of array
		assertFalse(lva.add(new DoubleValue(count)));
		assertFalse(lva.addAll(lva));

		// test copy
		assertEquals(lva, lva.copy());

		// test copyTo
		DoubleValueArray lvaTo = new DoubleValueArray();
		lva.copyTo(lvaTo);
		assertEquals(lva, lvaTo);

		// test clear
		lva.clear();
		assertEquals(0, lva.size());
	}

	@Test
	public void testUnboundedArray() {
		int count = 4096;

		ValueArray<DoubleValue> lva = new DoubleValueArray();

		// add several elements
		for (int i = 0; i < count; i++) {
			assertFalse(lva.isFull());
			assertEquals(i, lva.size());

			assertTrue(lva.add(new DoubleValue(i)));

			assertEquals(i + 1, lva.size());
		}

		// array never fills
		assertFalse(lva.isFull());
		assertEquals(count, lva.size());

		// verify the array values
		int idx = 0;
		for (DoubleValue lv : lva) {
			assertEquals(idx++, lv.getValue(), 0.000001);
		}

		// add element past end of array
		assertTrue(lva.add(new DoubleValue(count)));
		assertTrue(lva.addAll(lva));

		// test copy
		assertEquals(lva, lva.copy());

		// test copyTo
		DoubleValueArray lvaTo = new DoubleValueArray();
		lva.copyTo(lvaTo);
		assertEquals(lva, lvaTo);

		// test mark/reset
		int size = lva.size();
		lva.mark();
		assertTrue(lva.add(new DoubleValue()));
		assertEquals(size + 1, lva.size());
		lva.reset();
		assertEquals(size, lva.size());

		// test clear
		lva.clear();
		assertEquals(0, lva.size());
	}
}
