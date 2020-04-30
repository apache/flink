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
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LongValueToStringValue}.
 */
public class LongValueToStringValueTest {

	private TranslateFunction<LongValue, StringValue> translator = new LongValueToStringValue();

	@Test
	public void testTranslation() throws Exception {
		StringValue reuse = new StringValue();

		assertEquals(new StringValue("-9223372036854775808"), translator.translate(new LongValue(Long.MIN_VALUE), reuse));
		assertEquals(new StringValue("0"), translator.translate(new LongValue(0), reuse));
		assertEquals(new StringValue("9223372036854775807"), translator.translate(new LongValue(Long.MAX_VALUE), reuse));
	}
}
