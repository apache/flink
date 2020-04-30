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

import org.apache.flink.types.IntValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link IntValueArray}.
 */
public class IntValueArrayTest {

	@Test
	public void testBoundedArray() {
		int count = IntValueArray.DEFAULT_CAPACITY_IN_BYTES / IntValueArray.ELEMENT_LENGTH_IN_BYTES;

		ValueArray<IntValue> iva = new IntValueArray(IntValueArray.DEFAULT_CAPACITY_IN_BYTES);

		// fill the array
		for (int i = 0; i < count; i++) {
			assertFalse(iva.isFull());
			assertEquals(i, iva.size());

			assertTrue(iva.add(new IntValue(i)));

			assertEquals(i + 1, iva.size());
		}

		// array is now full
		assertTrue(iva.isFull());
		assertEquals(count, iva.size());

		// verify the array values
		int idx = 0;
		for (IntValue lv : iva) {
			assertEquals(idx++, lv.getValue());
		}

		// add element past end of array
		assertFalse(iva.add(new IntValue(count)));
		assertFalse(iva.addAll(iva));

		// test copy
		assertEquals(iva, iva.copy());

		// test copyTo
		IntValueArray ivaTo = new IntValueArray();
		iva.copyTo(ivaTo);
		assertEquals(iva, ivaTo);

		// test clear
		iva.clear();
		assertEquals(0, iva.size());
	}

	@Test
	public void testUnboundedArray() {
		int count = 4096;

		ValueArray<IntValue> iva = new IntValueArray();

		// add several elements
		for (int i = 0; i < count; i++) {
			assertFalse(iva.isFull());
			assertEquals(i, iva.size());

			assertTrue(iva.add(new IntValue(i)));

			assertEquals(i + 1, iva.size());
		}

		// array never fills
		assertFalse(iva.isFull());
		assertEquals(count, iva.size());

		// verify the array values
		int idx = 0;
		for (IntValue lv : iva) {
			assertEquals(idx++, lv.getValue());
		}

		// add element past end of array
		assertTrue(iva.add(new IntValue(count)));
		assertTrue(iva.addAll(iva));

		// test copy
		assertEquals(iva, iva.copy());

		// test copyTo
		IntValueArray ivaTo = new IntValueArray();
		iva.copyTo(ivaTo);
		assertEquals(iva, ivaTo);

		// test mark/reset
		int size = iva.size();
		iva.mark();
		assertTrue(iva.add(new IntValue()));
		assertEquals(size + 1, iva.size());
		iva.reset();
		assertEquals(size, iva.size());

		// test clear
		iva.clear();
		assertEquals(0, iva.size());
	}
}
