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

package org.apache.flink.util;

import org.junit.Test;

import static org.apache.flink.util.TernaryBoolean.FALSE;
import static org.apache.flink.util.TernaryBoolean.TRUE;
import static org.apache.flink.util.TernaryBoolean.UNDEFINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link TernaryBoolean} class.
 */
public class TernaryBooleanTest {

	@Test
	public void testWithDefault() {
		assertTrue(TRUE.getOrDefault(true));
		assertTrue(TRUE.getOrDefault(false));

		assertFalse(FALSE.getOrDefault(true));
		assertFalse(FALSE.getOrDefault(false));

		assertTrue(UNDEFINED.getOrDefault(true));
		assertFalse(UNDEFINED.getOrDefault(false));
	}

	@Test
	public void testResolveUndefined() {
		assertEquals(TRUE, TRUE.resolveUndefined(true));
		assertEquals(TRUE, TRUE.resolveUndefined(false));

		assertEquals(FALSE, FALSE.resolveUndefined(true));
		assertEquals(FALSE, FALSE.resolveUndefined(false));

		assertEquals(TRUE, UNDEFINED.resolveUndefined(true));
		assertEquals(FALSE, UNDEFINED.resolveUndefined(false));
	}

	@Test
	public void testToBoolean() {
		assertTrue(Boolean.TRUE == TRUE.getAsBoolean());
		assertTrue(Boolean.FALSE == FALSE.getAsBoolean());
		assertNull(UNDEFINED.getAsBoolean());
	}

	@Test
	public void testFromBoolean() {
		assertEquals(TRUE, TernaryBoolean.fromBoolean(true));
		assertEquals(FALSE, TernaryBoolean.fromBoolean(false));
	}

	@Test
	public void testFromBoxedBoolean() {
		assertEquals(TRUE, TernaryBoolean.fromBoxedBoolean(Boolean.TRUE));
		assertEquals(FALSE, TernaryBoolean.fromBoxedBoolean(Boolean.FALSE));
		assertEquals(UNDEFINED, TernaryBoolean.fromBoxedBoolean(null));
	}
}
