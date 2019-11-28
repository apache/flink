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

package org.apache.flink.api.common.resources;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Resource}.
 */
public class ResourceTest extends TestLogger {

	@Test
	public void testConstructorValid() {
		final Resource v1 = new TestResource(0.1);
		assertTestResourceValueEquals(0.1, v1);

		final Resource v2 = new TestResource(BigDecimal.valueOf(0.1));
		assertTestResourceValueEquals(0.1, v2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testConstructorInvalidValue() {
		new TestResource(-0.1);
	}

	@Test
	public void testEquals() {
		final Resource v1 = new TestResource(0.1);
		final Resource v2 = new TestResource(0.1);
		final Resource v3 = new TestResource(0.2);
		assertTrue(v1.equals(v2));
		assertFalse(v1.equals(v3));
	}

	@Test
	public void testEqualsIgnoringScale() {
		final Resource v1 = new TestResource(new BigDecimal("0.1"));
		final Resource v2 = new TestResource(new BigDecimal("0.10"));
		assertTrue(v1.equals(v2));
	}

	@Test
	public void testMerge() {
		final Resource v1 = new TestResource(0.1);
		final Resource v2 = new TestResource(0.2);
		assertTestResourceValueEquals(0.3, v1.merge(v2));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testMergeErrorOnDifferentTypes() {
		final Resource v1 = new TestResource(0.1);
		final Resource v2 = new GPUResource(0.1);
		v1.merge(v2);
	}

	@Test
	public void testSubtract() {
		final Resource v1 = new TestResource(0.2);
		final Resource v2 = new TestResource(0.1);
		assertTestResourceValueEquals(0.1, v1.subtract(v2));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSubtractLargerValue() {
		final Resource v1 = new TestResource(0.1);
		final Resource v2 = new TestResource(0.2);
		v1.subtract(v2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testSubtractErrorOnDifferentTypes() {
		final Resource v1 = new TestResource(0.1);
		final Resource v2 = new GPUResource(0.1);
		v1.subtract(v2);
	}

	private static void assertTestResourceValueEquals(final double value, final Resource resource) {
		assertEquals(new TestResource(value), resource);
	}
}
