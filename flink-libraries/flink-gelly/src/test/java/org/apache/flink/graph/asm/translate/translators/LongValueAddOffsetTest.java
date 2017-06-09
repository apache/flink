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

import org.apache.flink.types.LongValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LongValueAddOffset}.
 */
public class LongValueAddOffsetTest {

	@Test
	public void testTranslation() throws Exception {
		LongValue reuse = new LongValue();

		assertEquals(new LongValue(0), new LongValueAddOffset(0).translate(new LongValue(0), reuse));
		assertEquals(new LongValue(3), new LongValueAddOffset(1).translate(new LongValue(2), reuse));
		assertEquals(new LongValue(1), new LongValueAddOffset(-1).translate(new LongValue(2), reuse));

		assertEquals(new LongValue(-1), new LongValueAddOffset(Long.MIN_VALUE).translate(new LongValue(Long.MAX_VALUE), reuse));
		assertEquals(new LongValue(-1), new LongValueAddOffset(Long.MAX_VALUE).translate(new LongValue(Long.MIN_VALUE), reuse));

		// underflow wraps to positive values
		assertEquals(new LongValue(Long.MAX_VALUE), new LongValueAddOffset(-1).translate(new LongValue(Long.MIN_VALUE), reuse));
		assertEquals(new LongValue(0), new LongValueAddOffset(Long.MIN_VALUE).translate(new LongValue(Long.MIN_VALUE), reuse));

		// overflow wraps to negative values
		assertEquals(new LongValue(Long.MIN_VALUE), new LongValueAddOffset(1).translate(new LongValue(Long.MAX_VALUE), reuse));
		assertEquals(new LongValue(-2), new LongValueAddOffset(Long.MAX_VALUE).translate(new LongValue(Long.MAX_VALUE), reuse));
	}
}
