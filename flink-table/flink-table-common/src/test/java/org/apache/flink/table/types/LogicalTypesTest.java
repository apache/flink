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

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
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
			"VARCHAR(33)",
			new Class[]{String.class, byte[].class},
			new Class[]{String.class, byte[].class},
			new LogicalType[]{},
			new VarCharType(12)
		);
	}

	@Test
	public void testBooleanType() {
		testAll(
			new BooleanType(),
			"BOOLEAN",
			"BOOLEAN",
			new Class[]{Boolean.class, boolean.class},
			new Class[]{Boolean.class},
			new LogicalType[]{},
			new BooleanType(false)
		);
	}

	@Test
	public void testBinaryType() {
		testAll(
			new BinaryType(22),
			"BINARY(22)",
			"BINARY(22)",
			new Class[]{byte[].class},
			new Class[]{byte[].class},
			new LogicalType[]{},
			new BinaryType()
		);
	}

	@Test
	public void testVarBinaryType() {
		testAll(
			new VarBinaryType(22),
			"VARBINARY(22)",
			"VARBINARY(22)",
			new Class[]{byte[].class},
			new Class[]{byte[].class},
			new LogicalType[]{},
			new VarBinaryType()
		);
	}

	@Test
	public void testDecimalType() {
		testAll(
			new DecimalType(10, 2),
			"DECIMAL(10, 2)",
			"DECIMAL(10, 2)",
			new Class[]{BigDecimal.class},
			new Class[]{BigDecimal.class},
			new LogicalType[]{},
			new DecimalType()
		);
	}

	@Test
	public void testTinyIntType() {
		testAll(
			new TinyIntType(),
			"TINYINT",
			"TINYINT",
			new Class[]{Byte.class, byte.class},
			new Class[]{Byte.class},
			new LogicalType[]{},
			new TinyIntType(false)
		);
	}

	@Test
	public void testSmallIntType() {
		testAll(
			new SmallIntType(),
			"SMALLINT",
			"SMALLINT",
			new Class[]{Short.class, short.class},
			new Class[]{Short.class},
			new LogicalType[]{},
			new SmallIntType(false)
		);
	}

	@Test
	public void testIntType() {
		testAll(
			new IntType(),
			"INT",
			"INT",
			new Class[]{Integer.class, int.class},
			new Class[]{Integer.class},
			new LogicalType[]{},
			new IntType(false)
		);
	}

	@Test
	public void testBigIntType() {
		testAll(
			new BigIntType(),
			"BIGINT",
			"BIGINT",
			new Class[]{Long.class, long.class},
			new Class[]{Long.class},
			new LogicalType[]{},
			new BigIntType(false)
		);
	}

	@Test
	public void testFloatType() {
		testAll(
			new FloatType(),
			"FLOAT",
			"FLOAT",
			new Class[]{Float.class, float.class},
			new Class[]{Float.class},
			new LogicalType[]{},
			new FloatType(false)
		);
	}

	@Test
	public void testDoubleType() {
		testAll(
			new DoubleType(),
			"DOUBLE",
			"DOUBLE",
			new Class[]{Double.class, double.class},
			new Class[]{Double.class},
			new LogicalType[]{},
			new DoubleType(false)
		);
	}

	@Test
	public void testDateType() {
		testAll(
			new DateType(),
			"DATE",
			"DATE",
			new Class[]{java.sql.Date.class, java.time.LocalDate.class, int.class},
			new Class[]{java.time.LocalDate.class},
			new LogicalType[]{},
			new DateType(false)
		);
	}

	@Test
	public void testTimeType() {
		testAll(
			new TimeType(9),
			"TIME(9)",
			"TIME(9)",
			new Class[]{java.sql.Time.class, java.time.LocalTime.class, long.class},
			new Class[]{java.time.LocalTime.class},
			new LogicalType[]{},
			new TimeType()
		);
	}

	@Test
	public void testTimestampType() {
		testAll(
			new TimestampType(9),
			"TIMESTAMP(9)",
			"TIMESTAMP(9)",
			new Class[]{java.sql.Timestamp.class, java.time.LocalDateTime.class},
			new Class[]{java.time.LocalDateTime.class},
			new LogicalType[]{},
			new TimestampType(3)
		);
	}

	@Test
	public void testZonedTimestampType() {
		testAll(
			new ZonedTimestampType(9),
			"TIMESTAMP(9) WITH TIME ZONE",
			"TIMESTAMP(9) WITH TIME ZONE",
			new Class[]{java.time.ZonedDateTime.class, java.time.OffsetDateTime.class},
			new Class[]{java.time.OffsetDateTime.class},
			new LogicalType[]{},
			new ZonedTimestampType(3)
		);
	}

	@Test
	public void testLocalZonedTimestampType() {
		testAll(
			new LocalZonedTimestampType(9),
			"TIMESTAMP(9) WITH LOCAL TIME ZONE",
			"TIMESTAMP(9) WITH LOCAL TIME ZONE",
			new Class[]{java.time.Instant.class, long.class, int.class},
			new Class[]{java.time.Instant.class},
			new LogicalType[]{},
			new LocalZonedTimestampType(3)
		);
	}

	// --------------------------------------------------------------------------------------------

	private static void testAll(
		LogicalType nullableType,
		String serializableString,
		String summaryString,
		Class[] supportedInputClasses,
		Class[] supportedOutputClasses,
		LogicalType[] children,
		LogicalType otherType) {

		testEquality(nullableType, otherType);

		testNullability(nullableType);

		testJavaSerializability(nullableType);

		testStringSerializability(nullableType, serializableString);

		testStringSummary(nullableType, summaryString);

		testConversions(nullableType, supportedInputClasses, supportedOutputClasses);

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

	private static void testStringSerializability(LogicalType serializableType, String serializableString) {
		Assert.assertEquals(serializableString, serializableType.asSerializableString());
	}

	private static void testStringSummary(LogicalType type, String summaryString) {
		Assert.assertEquals(summaryString, type.asSummaryString());
	}

	private static void testConversions(LogicalType type, Class[] inputs, Class[] outputs) {
		for (Class<?> clazz : inputs) {
			assertTrue(type.supportsInputConversion(clazz));
		}

		for (Class<?> clazz : outputs) {
			assertTrue(type.supportsOutputConversion(clazz));
		}

		assertTrue(type.supportsInputConversion(type.getDefaultConversion()));

		assertTrue(type.supportsOutputConversion(type.getDefaultConversion()));

		assertFalse(type.supportsOutputConversion(LogicalTypesTest.class));

		assertFalse(type.supportsInputConversion(LogicalTypesTest.class));
	}

	private static void testChildren(LogicalType type, LogicalType[] children) {
		assertEquals(Arrays.asList(children), type.getChildren());
	}
}
