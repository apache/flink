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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;

import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;

/**
 * Migration tests for basic type serializers' snapshots.
 */
@RunWith(Parameterized.class)
public class BaseTypeSerializerSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Object> {

	public BaseTypeSerializerSnapshotMigrationTest(TestSpecification<Object> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<Object> testSpecifications() {

		// BigDecimal

		final TestSpecification<BigDecimal> bigDec = TestSpecification.<BigDecimal>builder(
				"1.6-big-dec",
				BigDecSerializer.class,
				BigDecSerializer.BigDecSerializerSnapshot.class)
			.withSerializerProvider(() -> BigDecSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-big-dec-serializer-snapshot")
			.withTestData("flink-1.6-big-dec-serializer-data", 10);

		// BigInteger

		final TestSpecification<BigInteger> bigInt = TestSpecification.<BigInteger>builder(
				"1.6-big-int",
				BigIntSerializer.class,
				BigIntSerializer.BigIntSerializerSnapshot.class)
			.withSerializerProvider(() -> BigIntSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-big-int-serializer-snapshot")
			.withTestData("flink-1.6-big-int-serializer-data", 10);

		// Boolean

		final TestSpecification<Boolean> booleanType = TestSpecification.<Boolean>builder(
				"1.6-boolean",
				BooleanSerializer.class,
				BooleanSerializer.BooleanSerializerSnapshot.class)
			.withSerializerProvider(() -> BooleanSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-boolean-serializer-snapshot")
			.withTestData("flink-1.6-boolean-serializer-data", 10);

		// BooleanValue

		final TestSpecification<BooleanValue> booleanValue = TestSpecification.<BooleanValue>builder(
				"1.6-boolean-value",
				BooleanValueSerializer.class,
				BooleanValueSerializer.BooleanValueSerializerSnapshot.class)
			.withSerializerProvider(() -> BooleanValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-boolean-value-serializer-snapshot")
			.withTestData("flink-1.6-boolean-value-serializer-data", 10);

		// Byte

		final TestSpecification<Byte> byteType = TestSpecification.<Byte>builder(
				"1.6-byte",
				ByteSerializer.class,
				ByteSerializer.ByteSerializerSnapshot.class)
			.withSerializerProvider(() -> ByteSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-byte-serializer-snapshot")
			.withTestData("flink-1.6-byte-serializer-data", 10);

		// ByteValue

		final TestSpecification<ByteValue> byteValue = TestSpecification.<ByteValue>builder(
				"1.6-byte-value",
				ByteValueSerializer.class,
				ByteValueSerializer.ByteValueSerializerSnapshot.class)
			.withSerializerProvider(() -> ByteValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-byte-value-serializer-snapshot")
			.withTestData("flink-1.6-byte-value-serializer-data", 10);

		// Character

		final TestSpecification<Character> charType = TestSpecification.<Character>builder(
				"1.6-char",
				CharSerializer.class,
				CharSerializer.CharSerializerSnapshot.class)
			.withSerializerProvider(() -> CharSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-char-serializer-snapshot")
			.withTestData("flink-1.6-char-serializer-data", 10);

		// CharValue

		final TestSpecification<CharValue> charValue = TestSpecification.<CharValue>builder(
				"1.6-char-value",
				CharValueSerializer.class,
				CharValueSerializer.CharValueSerializerSnapshot.class)
			.withSerializerProvider(() -> CharValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-char-value-serializer-snapshot")
			.withTestData("flink-1.6-char-value-serializer-data", 10);

		// java.util.Date

		final TestSpecification<Date> javaDate = TestSpecification.<Date>builder(
				"1.6-date",
				DateSerializer.class,
				DateSerializer.DateSerializerSnapshot.class)
			.withSerializerProvider(() -> DateSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-date-serializer-snapshot")
			.withTestData("flink-1.6-date-serializer-data", 10);

		// Double

		final TestSpecification<Double> doubleType = TestSpecification.<Double>builder(
				"1.6-double",
				DoubleSerializer.class,
				DoubleSerializer.DoubleSerializerSnapshot.class)
			.withSerializerProvider(() -> DoubleSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-double-serializer-snapshot")
			.withTestData("flink-1.6-double-serializer-data", 10);

		// DoubleValue

		final TestSpecification<DoubleValue> doubleValue = TestSpecification.<DoubleValue>builder(
				"1.6-double-value",
				DoubleValueSerializer.class,
				DoubleValueSerializer.DoubleValueSerializerSnapshot.class)
			.withSerializerProvider(() -> DoubleValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-double-value-serializer-snapshot")
			.withTestData("flink-1.6-double-value-serializer-data", 10);

		// Float

		final TestSpecification<Float> floatType = TestSpecification.<Float>builder(
				"1.6-float",
				FloatSerializer.class,
				FloatSerializer.FloatSerializerSnapshot.class)
			.withSerializerProvider(() -> FloatSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-float-serializer-snapshot")
			.withTestData("flink-1.6-float-serializer-data", 10);

		// FloatValue

		final TestSpecification<FloatValue> floatValue = TestSpecification.<FloatValue>builder(
				"1.6-float-value",
				FloatValueSerializer.class,
				FloatValueSerializer.FloatValueSerializerSnapshot.class)
			.withSerializerProvider(() -> FloatValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-float-value-serializer-snapshot")
			.withTestData("flink-1.6-float-value-serializer-data", 10);

		// Integer

		final TestSpecification<Integer> intType = TestSpecification.<Integer>builder(
				"1.6-int",
				IntSerializer.class,
				IntSerializer.IntSerializerSnapshot.class)
			.withSerializerProvider(() -> IntSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-int-serializer-snapshot")
			.withTestData("flink-1.6-int-serializer-data", 10);

		// IntValue

		final TestSpecification<IntValue> intValue = TestSpecification.<IntValue>builder(
				"1.6-int-value",
				IntValueSerializer.class,
				IntValueSerializer.IntValueSerializerSnapshot.class)
			.withSerializerProvider(() -> IntValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-int-value-serializer-snapshot")
			.withTestData("flink-1.6-int-value-serializer-data", 10);

		// Long

		final TestSpecification<Long> longType = TestSpecification.<Long>builder(
				"1.6-long",
				LongSerializer.class,
				LongSerializer.LongSerializerSnapshot.class)
			.withSerializerProvider(() -> LongSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-long-serializer-snapshot")
			.withTestData("flink-1.6-long-serializer-data", 10);

		// LongValue

		final TestSpecification<LongValue> longValue = TestSpecification.<LongValue>builder(
				"1.6-long-value",
				LongValueSerializer.class,
				LongValueSerializer.LongValueSerializerSnapshot.class)
			.withSerializerProvider(() -> LongValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-long-value-serializer-snapshot")
			.withTestData("flink-1.6-long-value-serializer-data", 10);

		// NullValue

		final TestSpecification<NullValue> nullValue = TestSpecification.<NullValue>builder(
				"1.6-null-value",
				NullValueSerializer.class,
				NullValueSerializer.NullValueSerializerSnapshot.class)
			.withSerializerProvider(() -> NullValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-null-value-serializer-snapshot")
			.withTestData("flink-1.6-null-value-serializer-data", 10);

		// Short

		final TestSpecification<Short> shortType = TestSpecification.<Short>builder(
				"1.6-short",
				ShortSerializer.class,
				ShortSerializer.ShortSerializerSnapshot.class)
			.withSerializerProvider(() -> ShortSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-short-serializer-snapshot")
			.withTestData("flink-1.6-short-serializer-data", 10);

		// ShortValue

		final TestSpecification<ShortValue> shortValue = TestSpecification.<ShortValue>builder(
				"1.6-short-value",
				ShortValueSerializer.class,
				ShortValueSerializer.ShortValueSerializerSnapshot.class)
			.withSerializerProvider(() -> ShortValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-short-value-serializer-snapshot")
			.withTestData("flink-1.6-short-value-serializer-data", 10);

		// java.sql.Date

		final TestSpecification<java.sql.Date> sqlDate = TestSpecification.<java.sql.Date>builder(
				"1.6-sql-date",
				SqlDateSerializer.class,
				SqlDateSerializer.SqlDateSerializerSnapshot.class)
			.withSerializerProvider(() -> SqlDateSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-sql-date-serializer-snapshot")
			.withTestData("flink-1.6-sql-date-serializer-data", 10);

		// java.sql.Time

		final TestSpecification<Time> sqlTime = TestSpecification.<Time>builder(
				"1.6-sql-time",
				SqlTimeSerializer.class,
				SqlTimeSerializer.SqlTimeSerializerSnapshot.class)
			.withSerializerProvider(() -> SqlTimeSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-sql-time-serializer-snapshot")
			.withTestData("flink-1.6-sql-time-serializer-data", 10);

		// java.sql.Timestamp

		final TestSpecification<Timestamp> sqlTimestamp = TestSpecification.<Timestamp>builder(
				"1.6-sql-timestamp",
				SqlTimestampSerializer.class,
				SqlTimestampSerializer.SqlTimestampSerializerSnapshot.class)
			.withSerializerProvider(() -> SqlTimestampSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-sql-timestamp-serializer-snapshot")
			.withTestData("flink-1.6-sql-timestamp-serializer-data", 10);

		// String

		final TestSpecification<String> stringType = TestSpecification.<String>builder(
				"1.6-string",
				StringSerializer.class,
				StringSerializer.StringSerializerSnapshot.class)
			.withSerializerProvider(() -> StringSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-string-serializer-snapshot")
			.withTestData("flink-1.6-string-serializer-data", 10);

		// StringValue

		final TestSpecification<StringValue> stringValue = TestSpecification.<StringValue>builder(
				"1.6-string-value",
				StringValueSerializer.class,
				StringValueSerializer.StringValueSerializerSnapshot.class)
			.withSerializerProvider(() -> StringValueSerializer.INSTANCE)
			.withSnapshotDataLocation("flink-1.6-string-value-serializer-snapshot")
			.withTestData("flink-1.6-string-value-serializer-data", 10);

		return Arrays.asList(
			bigDec,
			bigInt,
			booleanType,
			booleanValue,
			byteType,
			byteValue,
			charType,
			charValue,
			javaDate,
			doubleType,
			doubleValue,
			floatType,
			floatValue,
			intType,
			intValue,
			longType,
			longValue,
			nullValue,
			shortType,
			shortValue,
			sqlDate,
			sqlTime,
			sqlTimestamp,
			stringType,
			stringValue
		);
	}
}
