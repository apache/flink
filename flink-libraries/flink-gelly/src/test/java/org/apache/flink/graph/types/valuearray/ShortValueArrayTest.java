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

import org.apache.flink.types.ShortValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link ShortValueArray}.
 */
public class ShortValueArrayTest {

	@Test
	public void testBoundedArray() {
		int count = ShortValueArray.DEFAULT_CAPACITY_IN_BYTES / ShortValueArray.ELEMENT_LENGTH_IN_BYTES;

		ValueArray<ShortValue> lva = new ShortValueArray(ShortValueArray.DEFAULT_CAPACITY_IN_BYTES);

		// fill the array
		for (int i = 0; i < count; i++) {
			assertFalse(lva.isFull());
			assertEquals(i, lva.size());

			assertTrue(lva.add(new ShortValue((short) i)));

			assertEquals(i + 1, lva.size());
		}

		// array is now full
		assertTrue(lva.isFull());
		assertEquals(count, lva.size());

		// verify the array values
		int idx = 0;
		for (ShortValue lv : lva) {
			assertEquals((short) idx++, lv.getValue());
		}

		// add element past end of array
		assertFalse(lva.add(new ShortValue((short) count)));
		assertFalse(lva.addAll(lva));

		// test copy
		assertEquals(lva, lva.copy());

		// test copyTo
		ShortValueArray lvaTo = new ShortValueArray();
		lva.copyTo(lvaTo);
		assertEquals(lva, lvaTo);

		// test clear
		lva.clear();
		assertEquals(0, lva.size());
	}

	@Test
	public void testUnboundedArray() {
		int count = 4096;

		ValueArray<ShortValue> lva = new ShortValueArray();

		// add several elements
		for (int i = 0; i < count; i++) {
			assertFalse(lva.isFull());
			assertEquals(i, lva.size());

			assertTrue(lva.add(new ShortValue((short) i)));

			assertEquals(i + 1, lva.size());
		}

		// array never fills
		assertFalse(lva.isFull());
		assertEquals(count, lva.size());

		// verify the array values
		int idx = 0;
		for (ShortValue lv : lva) {
			assertEquals((short) idx++, lv.getValue());
		}

		// add element past end of array
		assertTrue(lva.add(new ShortValue((short) count)));
		assertTrue(lva.addAll(lva));

		// test copy
		assertEquals(lva, lva.copy());

		// test copyTo
		ShortValueArray lvaTo = new ShortValueArray();
		lva.copyTo(lvaTo);
		assertEquals(lva, lvaTo);

		// test mark/reset
		int size = lva.size();
		lva.mark();
		assertTrue(lva.add(new ShortValue()));
		assertEquals(size + 1, lva.size());
		lva.reset();
		assertEquals(size, lva.size());

		// test clear
		lva.clear();
		assertEquals(0, lva.size());
	}
}
