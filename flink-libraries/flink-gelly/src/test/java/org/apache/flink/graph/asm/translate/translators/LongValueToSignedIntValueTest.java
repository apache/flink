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

package org.apache.flink.graph.asm.translate.translators;

import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LongValueToSignedIntValue}.
 */
public class LongValueToSignedIntValueTest {

	private TranslateFunction<LongValue, IntValue> translator = new LongValueToSignedIntValue();

	private IntValue reuse = new IntValue();

	@Test
	public void testTranslation() throws Exception {
		assertEquals(new IntValue(Integer.MIN_VALUE), translator.translate(new LongValue((long) Integer.MIN_VALUE), reuse));
		assertEquals(new IntValue(0), translator.translate(new LongValue(0L), reuse));
		assertEquals(new IntValue(Integer.MAX_VALUE), translator.translate(new LongValue((long) Integer.MAX_VALUE), reuse));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUpperOutOfRange() throws Exception {
		translator.translate(new LongValue((long) Integer.MAX_VALUE + 1), reuse);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testLowerOutOfRange() throws Exception {
		translator.translate(new LongValue((long) Integer.MIN_VALUE - 1), reuse);
	}
}
