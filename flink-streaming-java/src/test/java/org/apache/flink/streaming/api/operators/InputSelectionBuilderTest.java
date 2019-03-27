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

/**
 * Test for {@link Builder}.
 */
public class InputSelectionBuilderTest {

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
