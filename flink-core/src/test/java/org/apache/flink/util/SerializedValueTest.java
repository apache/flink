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

import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SerializedValue}.
 */
public class SerializedValueTest {

	@Test
	public void testSimpleValue() {
		try {
			final String value = "teststring";

			SerializedValue<String> v = new SerializedValue<>(value);
			SerializedValue<String> copy = CommonTestUtils.createCopySerializable(v);

			assertEquals(value, v.deserializeValue(getClass().getClassLoader()));
			assertEquals(value, copy.deserializeValue(getClass().getClassLoader()));

			assertEquals(v, copy);
			assertEquals(v.hashCode(), copy.hashCode());

			assertNotNull(v.toString());
			assertNotNull(copy.toString());

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testNullValue() {
		try {
			SerializedValue<Object> v = new SerializedValue<>(null);
			SerializedValue<Object> copy = CommonTestUtils.createCopySerializable(v);

			assertNull(copy.deserializeValue(getClass().getClassLoader()));

			assertEquals(v, copy);
			assertEquals(v.hashCode(), copy.hashCode());
			assertEquals(v.toString(), copy.toString());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
