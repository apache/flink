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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.operators.InputSelection.Builder;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link InputSelection}.
 */
public class InputSelectionTest {

	@Test
	public void testIsInputSelected() {
		assertFalse(new Builder().build().isInputSelected(1));
		assertFalse(new Builder().select(2).build().isInputSelected(1));

		assertTrue(new Builder().select(1).build().isInputSelected(1));
		assertTrue(new Builder().select(1).select(2).build().isInputSelected(1));
		assertTrue(new Builder().select(-1).build().isInputSelected(1));

		assertTrue(new Builder().select(64).build().isInputSelected(64));
	}

	@Test
	public void testIsALLMaskOf2() {
		assertTrue(InputSelection.ALL.isALLMaskOf2());
		assertTrue(new Builder().select(1).select(2).build().isALLMaskOf2());

		assertFalse(InputSelection.FIRST.isALLMaskOf2());
		assertFalse(InputSelection.SECOND.isALLMaskOf2());
		assertFalse(new Builder().select(3).build().isALLMaskOf2());
	}

	@Test
	public void testFairSelectNextIndexOutOf2() {
		assertEquals(1, InputSelection.ALL.fairSelectNextIndexOutOf2(3, 0));
		assertEquals(0, new Builder().select(1).select(2).build().fairSelectNextIndexOutOf2(3, 1));

		assertEquals(1, InputSelection.ALL.fairSelectNextIndexOutOf2(2, 0));
		assertEquals(1, InputSelection.ALL.fairSelectNextIndexOutOf2(2, 1));
		assertEquals(0, InputSelection.ALL.fairSelectNextIndexOutOf2(1, 0));
		assertEquals(0, InputSelection.ALL.fairSelectNextIndexOutOf2(1, 1));
		assertEquals(-1, InputSelection.ALL.fairSelectNextIndexOutOf2(0, 0));
		assertEquals(-1, InputSelection.ALL.fairSelectNextIndexOutOf2(0, 1));

		assertEquals(0, InputSelection.FIRST.fairSelectNextIndexOutOf2(1, 0));
		assertEquals(0, InputSelection.FIRST.fairSelectNextIndexOutOf2(3, 0));
		assertEquals(-1, InputSelection.FIRST.fairSelectNextIndexOutOf2(2, 0));
		assertEquals(-1, InputSelection.FIRST.fairSelectNextIndexOutOf2(0, 0));

		assertEquals(1, InputSelection.SECOND.fairSelectNextIndexOutOf2(2, 1));
		assertEquals(1, InputSelection.SECOND.fairSelectNextIndexOutOf2(3, 1));
		assertEquals(-1, InputSelection.SECOND.fairSelectNextIndexOutOf2(1, 1));
		assertEquals(-1, InputSelection.SECOND.fairSelectNextIndexOutOf2(0, 1));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedFairSelectNextIndexOutOf2() {
		InputSelection.ALL.fairSelectNextIndexOutOf2(7, 0);
	}

	/**
	 * Tests for {@link Builder}.
	 */
	public static class BuilderTest {

		@Test
		public void testSelect() {
			assertEquals(1L, new Builder().select(1).build().getInputMask());
			assertEquals(7L, new Builder().select(1).select(2).select(3).build().getInputMask());

			assertEquals(0x8000_0000_0000_0000L, new Builder().select(64).build().getInputMask());
			assertEquals(0xffff_ffff_ffff_ffffL, new Builder().select(-1).build().getInputMask());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testIllegalInputId1() {
			new Builder().select(-2);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testIllegalInputId2() {
			new Builder().select(65);
		}
	}
}
