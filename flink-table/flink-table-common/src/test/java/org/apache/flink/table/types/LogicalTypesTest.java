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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationAnyType;
import org.apache.flink.table.types.logical.UserDefinedType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

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

	@Test
	public void testYearMonthIntervalType() {
		testAll(
			new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH, 2),
			"INTERVAL YEAR(2) TO MONTH",
			"INTERVAL YEAR(2) TO MONTH",
			new Class[]{java.time.Period.class, int.class},
			new Class[]{java.time.Period.class},
			new LogicalType[]{},
			new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.MONTH)
		);
	}

	@Test
	public void testDayTimeIntervalType() {
		testAll(
			new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 6),
			"INTERVAL DAY(2) TO SECOND(6)",
			"INTERVAL DAY(2) TO SECOND(6)",
			new Class[]{java.time.Duration.class, long.class},
			new Class[]{java.time.Duration.class},
			new LogicalType[]{},
			new DayTimeIntervalType(DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 7)
		);
	}

	@Test
	public void testArrayType() {
		testAll(
			new ArrayType(new TimestampType()),
			"ARRAY<TIMESTAMP(6)>",
			"ARRAY<TIMESTAMP(6)>",
			new Class[]{java.sql.Timestamp[].class, java.time.LocalDateTime[].class},
			new Class[]{java.sql.Timestamp[].class, java.time.LocalDateTime[].class},
			new LogicalType[]{new TimestampType()},
			new ArrayType(new SmallIntType())
		);

		testAll(
			new ArrayType(new ArrayType(new TimestampType())),
			"ARRAY<ARRAY<TIMESTAMP(6)>>",
			"ARRAY<ARRAY<TIMESTAMP(6)>>",
			new Class[]{java.sql.Timestamp[][].class, java.time.LocalDateTime[][].class},
			new Class[]{java.sql.Timestamp[][].class, java.time.LocalDateTime[][].class},
			new LogicalType[]{new ArrayType(new TimestampType())},
			new ArrayType(new ArrayType(new SmallIntType()))
		);

		final LogicalType nestedArray = new ArrayType(new ArrayType(new TimestampType()));
		assertFalse(nestedArray.supportsInputConversion(java.sql.Timestamp[].class));
		assertFalse(nestedArray.supportsOutputConversion(java.sql.Timestamp[].class));
	}

	@Test
	public void testMultisetType() {
		testAll(
			new MultisetType(new TimestampType()),
			"MULTISET<TIMESTAMP(6)>",
			"MULTISET<TIMESTAMP(6)>",
			new Class[]{Map.class},
			new Class[]{Map.class},
			new LogicalType[]{new TimestampType()},
			new MultisetType(new SmallIntType())
		);

		testAll(
			new MultisetType(new MultisetType(new TimestampType())),
			"MULTISET<MULTISET<TIMESTAMP(6)>>",
			"MULTISET<MULTISET<TIMESTAMP(6)>>",
			new Class[]{Map.class},
			new Class[]{Map.class},
			new LogicalType[]{new MultisetType(new TimestampType())},
			new MultisetType(new MultisetType(new SmallIntType()))
		);
	}

	@Test
	public void testMapType() {
		testAll(
			new MapType(new VarCharType(20), new TimestampType()),
			"MAP<VARCHAR(20), TIMESTAMP(6)>",
			"MAP<VARCHAR(20), TIMESTAMP(6)>",
			new Class[]{Map.class},
			new Class[]{Map.class},
			new LogicalType[]{new VarCharType(20), new TimestampType()},
			new MapType(new VarCharType(99), new TimestampType())
		);
	}

	@Test
	public void testRowType() {
		testAll(
			new RowType(
				Arrays.asList(
					new RowType.RowField("a", new VarCharType(), "Someone's desc."),
					new RowType.RowField("b`", new TimestampType()))),
			"ROW<`a` VARCHAR(1) 'Someone''s desc.', `b``` TIMESTAMP(6)>",
			"ROW<`a` VARCHAR(1) '...', `b``` TIMESTAMP(6)>",
			new Class[]{Row.class},
			new Class[]{Row.class},
			new LogicalType[]{new VarCharType(), new TimestampType()},
			new RowType(
				Arrays.asList(
					new RowType.RowField("a", new VarCharType(), "Different desc."),
					new RowType.RowField("b`", new TimestampType())))
		);
	}

	@Test
	public void testDistinctType() {
		testAll(
			createDistinctType("Money"),
			"`cat`.`db`.`Money`",
			"`cat`.`db`.`Money`",
			new Class[]{BigDecimal.class},
			new Class[]{BigDecimal.class},
			new LogicalType[]{new DecimalType(10, 2)},
			createDistinctType("Monetary")
		);
	}

	@Test
	public void testStructuredType() {
		testAll(
			createUserType(true),
			"`cat`.`db`.`User`",
			"`cat`.`db`.`User`",
			new Class[]{Row.class, User.class},
			new Class[]{Row.class, Human.class, User.class},
			new LogicalType[]{UDT_NAME_TYPE, UDT_SETTING_TYPE},
			createUserType(false)
		);

		testConversions(
			createHumanType(false),
			new Class[]{Row.class, Human.class, User.class}, // every User is Human
			new Class[]{Row.class, Human.class});

		// not every Human is User
		assertFalse(createUserType(true).supportsInputConversion(Human.class));

		// User is not implementing SpecialHuman
		assertFalse(createHumanType(true).supportsInputConversion(User.class));
	}

	@Test
	public void testNullType() {
		final NullType nullType = new NullType();

		testEquality(nullType, new TimeType());

		testJavaSerializability(nullType);

		testStringSerializability(nullType, "NULL");

		testStringSummary(nullType, "NULL");

		assertTrue(nullType.supportsInputConversion(Object.class));

		assertTrue(nullType.supportsOutputConversion(Object.class));

		assertTrue(nullType.supportsOutputConversion(Integer.class));

		assertFalse(nullType.supportsOutputConversion(int.class));
	}

	@Test
	public void testTypeInformationAnyType() {
		final TypeInformationAnyType<?> anyType = new TypeInformationAnyType<>(Types.TUPLE(Types.STRING, Types.INT));

		testEquality(anyType, new TypeInformationAnyType<>(Types.TUPLE(Types.STRING, Types.LONG)));

		testStringSummary(anyType, "ANY(org.apache.flink.api.java.tuple.Tuple2, ?)");

		testNullability(anyType);

		testJavaSerializability(anyType);

		testConversions(anyType, new Class[]{Tuple2.class}, new Class[]{Tuple.class});
	}

	@Test
	public void testAnyType() {
		testAll(
			new AnyType<>(Human.class, new KryoSerializer<>(Human.class, new ExecutionConfig())),
				"ANY(org.apache.flink.table.types.LogicalTypesTest$Human, " +
					"ADNvcmcuYXBhY2hlLmZsaW5rLnRhYmxlLnR5cGVzLkxvZ2ljYWxUeXBlc1Rlc3QkSHVtYW4AAATyxpo9cAA" +
					"AAAIAM29yZy5hcGFjaGUuZmxpbmsudGFibGUudHlwZXMuTG9naWNhbFR5cGVzVGVzdCRIdW1hbgEAAAA1AD" +
					"NvcmcuYXBhY2hlLmZsaW5rLnRhYmxlLnR5cGVzLkxvZ2ljYWxUeXBlc1Rlc3QkSHVtYW4BAAAAOQAzb3JnL" +
					"mFwYWNoZS5mbGluay50YWJsZS50eXBlcy5Mb2dpY2FsVHlwZXNUZXN0JEh1bWFuAAAAAAApb3JnLmFwYWNo" +
					"ZS5hdnJvLmdlbmVyaWMuR2VuZXJpY0RhdGEkQXJyYXkBAAAAKwApb3JnLmFwYWNoZS5hdnJvLmdlbmVyaWM" +
					"uR2VuZXJpY0RhdGEkQXJyYXkBAAAAtgBVb3JnLmFwYWNoZS5mbGluay5hcGkuamF2YS50eXBldXRpbHMucn" +
					"VudGltZS5rcnlvLlNlcmlhbGl6ZXJzJER1bW15QXZyb1JlZ2lzdGVyZWRDbGFzcwAAAAEAWW9yZy5hcGFja" +
					"GUuZmxpbmsuYXBpLmphdmEudHlwZXV0aWxzLnJ1bnRpbWUua3J5by5TZXJpYWxpemVycyREdW1teUF2cm9L" +
					"cnlvU2VyaWFsaXplckNsYXNzAAAE8saaPXAAAAAAAAAE8saaPXAAAAAA)",
			"ANY(org.apache.flink.table.types.LogicalTypesTest$Human, ...)",
			new Class[]{Human.class, User.class}, // every User is Human
			new Class[]{Human.class},
			new LogicalType[]{},
			new AnyType<>(User.class, new KryoSerializer<>(User.class, new ExecutionConfig()))
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

	private DistinctType createDistinctType(String typeName) {
		return new DistinctType.Builder(
				new UserDefinedType.TypeIdentifier("cat", "db", typeName),
				new DecimalType(10, 2))
			.setDescription("Money type desc.")
			.build();
	}

	private static final LogicalType UDT_NAME_TYPE = new VarCharType();

	private static final LogicalType UDT_SETTING_TYPE = new IntType();

	private StructuredType createHumanType(boolean useDifferentImplementation) {
		return new StructuredType.Builder(
				new UserDefinedType.TypeIdentifier("cat", "db", "Human"),
				Collections.singletonList(
					new StructuredType.StructuredAttribute("name", UDT_NAME_TYPE, "Description.")))
			.setDescription("Human type desc.")
			.setFinal(false)
			.setInstantiable(false)
			.setImplementationClass(useDifferentImplementation ? SpecialHuman.class : Human.class)
			.build();
	}

	private StructuredType createUserType(boolean isFinal) {
		return new StructuredType.Builder(
				new UserDefinedType.TypeIdentifier("cat", "db", "User"),
				Collections.singletonList(
					new StructuredType.StructuredAttribute("setting", UDT_SETTING_TYPE)))
			.setDescription("User type desc.")
			.setFinal(isFinal)
			.setInstantiable(true)
			.setImplementationClass(User.class)
			.setSuperType(createHumanType(false))
			.build();
	}

	private abstract static class SpecialHuman {
		public String name;
	}

	private abstract static class Human {
		public String name;
	}

	private static final class User extends Human {
		public int setting;
	}
}
