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

package org.apache.flink.table.types;

import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for subclasses of {@link org.apache.flink.table.types.logical.LogicalType}.
 */
public class LogicalTypesTest {

	@Test
	public void testCharType() {
		testAll(
			new CharType(33),
			"CHAR(33)",
			new Class[]{String.class, byte[].class},
			new Class[]{String.class, byte[].class},
			new LogicalType[]{},
			new CharType(12)
		);
	}

	@Test
	public void testVarCharType() {
		testAll(
			new VarCharType(33),
			"VARCHAR(33)",
			new Class[]{String.class, byte[].class},
			new Class[]{String.class, byte[].class},
			new LogicalType[]{},
			new VarCharType(12)
		);
	}

	// --------------------------------------------------------------------------------------------

	private static void testAll(
		LogicalType nullableType,
		String typeString,
		Class[] input,
		Class[] output,
		LogicalType[] children,
		LogicalType otherType) {

		testEquality(nullableType, otherType);

		testNullability(nullableType);

		testJavaSerializability(nullableType);

		testStringSerializability(nullableType, typeString);

		testConversions(nullableType, input, output);

		testChildren(nullableType, children);
	}

	private static void testEquality(LogicalType nullableType, LogicalType otherType) {
		assertTrue(nullableType.isNullable());

		assertEquals(nullableType, nullableType);
		assertEquals(nullableType.hashCode(), nullableType.hashCode());

		assertEquals(nullableType, nullableType.copy());

		assertNotEquals(nullableType, otherType);
		assertNotEquals(nullableType.hashCode(), otherType.hashCode());
	}

	private static void testNullability(LogicalType nullableType) {
		final LogicalType notNullInstance = nullableType.copy(false);

		assertNotEquals(nullableType, notNullInstance);

		assertFalse(notNullInstance.isNullable());
	}

	private static void testJavaSerializability(LogicalType serializableType) {
		try {
			final LogicalType deserializedInstance = InstantiationUtil.deserializeObject(
				InstantiationUtil.serializeObject(serializableType),
				LogicalTypesTest.class.getClassLoader());

			assertEquals(serializableType, deserializedInstance);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static void testStringSerializability(LogicalType serializableType, String typeString) {
		Assert.assertEquals(typeString, serializableType.asSerializableString());
	}

	private static void testConversions(LogicalType type, Class[] inputs, Class[] outputs) {
		for (Class<?> clazz : inputs) {
			assertTrue(type.supportsInputConversion(clazz));
		}

		for (Class<?> clazz : outputs) {
			assertTrue(type.supportsOutputConversion(clazz));
		}

		assertTrue(type.supportsOutputConversion(type.getDefaultOutputConversion()));

		assertFalse(type.supportsOutputConversion(LogicalTypesTest.class));

		assertFalse(type.supportsInputConversion(LogicalTypesTest.class));
	}

	private static void testChildren(LogicalType type, LogicalType[] children) {
		assertEquals(Arrays.asList(children), type.getChildren());
	}
}
