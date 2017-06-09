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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.types.LongValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link LongValueArray}.
 */
public class LongValueArrayTest {

	@Test
	public void testBoundedArray() {
		int count = LongValueArray.DEFAULT_CAPACITY_IN_BYTES / LongValueArray.ELEMENT_LENGTH_IN_BYTES;

		ValueArray<LongValue> lva = new LongValueArray(LongValueArray.DEFAULT_CAPACITY_IN_BYTES);

		// fill the array
		for (int i = 0; i < count; i++) {
			assertFalse(lva.isFull());
			assertEquals(i, lva.size());

			assertTrue(lva.add(new LongValue(i)));

			assertEquals(i + 1, lva.size());
		}

		// array is now full
		assertTrue(lva.isFull());
		assertEquals(count, lva.size());

		// verify the array values
		int idx = 0;
		for (LongValue lv : lva) {
			assertEquals(idx++, lv.getValue());
		}

		// add element past end of array
		assertFalse(lva.add(new LongValue(count)));
		assertFalse(lva.addAll(lva));

		// test copy
		assertEquals(lva, lva.copy());

		// test copyTo
		LongValueArray lvaTo = new LongValueArray();
		lva.copyTo(lvaTo);
		assertEquals(lva, lvaTo);

		// test clear
		lva.clear();
		assertEquals(0, lva.size());
	}

	@Test
	public void testUnboundedArray() {
		int count = 4096;

		ValueArray<LongValue> lva = new LongValueArray();

		// add several elements
		for (int i = 0; i < count; i++) {
			assertFalse(lva.isFull());
			assertEquals(i, lva.size());

			assertTrue(lva.add(new LongValue(i)));

			assertEquals(i + 1, lva.size());
		}

		// array never fills
		assertFalse(lva.isFull());
		assertEquals(count, lva.size());

		// verify the array values
		int idx = 0;
		for (LongValue lv : lva) {
			assertEquals(idx++, lv.getValue());
		}

		// add element past end of array
		assertTrue(lva.add(new LongValue(count)));
		assertTrue(lva.addAll(lva));

		// test copy
		assertEquals(lva, lva.copy());

		// test copyTo
		LongValueArray lvaTo = new LongValueArray();
		lva.copyTo(lvaTo);
		assertEquals(lva, lvaTo);

		// test mark/reset
		int size = lva.size();
		lva.mark();
		assertTrue(lva.add(new LongValue()));
		assertEquals(size + 1, lva.size());
		lva.reset();
		assertEquals(size, lva.size());

		// test clear
		lva.clear();
		assertEquals(0, lva.size());
	}
}
