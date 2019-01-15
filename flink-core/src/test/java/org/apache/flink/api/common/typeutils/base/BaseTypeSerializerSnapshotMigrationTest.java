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
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

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
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add("big-dec-serializer", BigDecSerializer.class, BigDecSerializer.BigDecSerializerSnapshot.class, () -> BigDecSerializer.INSTANCE);
		testSpecifications.add("big-int-serializer", BigIntSerializer.class, BigIntSerializer.BigIntSerializerSnapshot.class, () -> BigIntSerializer.INSTANCE);
		testSpecifications.add("boolean-serializer", BooleanSerializer.class, BooleanSerializer.BooleanSerializerSnapshot.class, () -> BooleanSerializer.INSTANCE);
		testSpecifications.add("boolean-value-serializer", BooleanValueSerializer.class, BooleanValueSerializer.BooleanValueSerializerSnapshot.class, () -> BooleanValueSerializer.INSTANCE);
		testSpecifications.add("byte-serializer", ByteSerializer.class, ByteSerializer.ByteSerializerSnapshot.class, () -> ByteSerializer.INSTANCE);
		testSpecifications.add("byte-value-serializer", ByteValueSerializer.class, ByteValueSerializer.ByteValueSerializerSnapshot.class, () -> ByteValueSerializer.INSTANCE);
		testSpecifications.add("char-serializer", CharSerializer.class, CharSerializer.CharSerializerSnapshot.class, () -> CharSerializer.INSTANCE);
		testSpecifications.add("char-value-serializer", CharValueSerializer.class, CharValueSerializer.CharValueSerializerSnapshot.class, () -> CharValueSerializer.INSTANCE);
		testSpecifications.add("date-serializer", DateSerializer.class, DateSerializer.DateSerializerSnapshot.class, () -> DateSerializer.INSTANCE);
		testSpecifications.add("double-serializer", DoubleSerializer.class, DoubleSerializer.DoubleSerializerSnapshot.class, () -> DoubleSerializer.INSTANCE);
		testSpecifications.add("double-value-serializer", DoubleValueSerializer.class, DoubleValueSerializer.DoubleValueSerializerSnapshot.class, () -> DoubleValueSerializer.INSTANCE);
		testSpecifications.add("float-serializer", FloatSerializer.class, FloatSerializer.FloatSerializerSnapshot.class, () -> FloatSerializer.INSTANCE);
		testSpecifications.add("float-value-serializer", FloatValueSerializer.class, FloatValueSerializer.FloatValueSerializerSnapshot.class, () -> FloatValueSerializer.INSTANCE);
		testSpecifications.add("int-serializer", IntSerializer.class, IntSerializer.IntSerializerSnapshot.class, () -> IntSerializer.INSTANCE);
		testSpecifications.add("int-value-serializer", IntValueSerializer.class, IntValueSerializer.IntValueSerializerSnapshot.class, () -> IntValueSerializer.INSTANCE);
		testSpecifications.add("long-serializer", LongSerializer.class, LongSerializer.LongSerializerSnapshot.class, () -> LongSerializer.INSTANCE);
		testSpecifications.add("long-value-serializer", LongValueSerializer.class, LongValueSerializer.LongValueSerializerSnapshot.class, () -> LongValueSerializer.INSTANCE);
		testSpecifications.add("null-value-serializer", NullValueSerializer.class, NullValueSerializer.NullValueSerializerSnapshot.class, () -> NullValueSerializer.INSTANCE);
		testSpecifications.add("short-serializer", ShortSerializer.class, ShortSerializer.ShortSerializerSnapshot.class, () -> ShortSerializer.INSTANCE);
		testSpecifications.add("short-value-serializer", ShortValueSerializer.class, ShortValueSerializer.ShortValueSerializerSnapshot.class, () -> ShortValueSerializer.INSTANCE);
		testSpecifications.add("sql-date-serializer", SqlDateSerializer.class, SqlDateSerializer.SqlDateSerializerSnapshot.class, () -> SqlDateSerializer.INSTANCE);
		testSpecifications.add("sql-time-serializer", SqlTimeSerializer.class, SqlTimeSerializer.SqlTimeSerializerSnapshot.class, () -> SqlTimeSerializer.INSTANCE);
		testSpecifications.add("sql-timestamp-serializer", SqlTimestampSerializer.class, SqlTimestampSerializer.SqlTimestampSerializerSnapshot.class, () -> SqlTimestampSerializer.INSTANCE);
		testSpecifications.add("string-serializer", StringSerializer.class, StringSerializer.StringSerializerSnapshot.class, () -> StringSerializer.INSTANCE);
		testSpecifications.add("string-value-serializer", StringValueSerializer.class, StringValueSerializer.StringValueSerializerSnapshot.class, () -> StringValueSerializer.INSTANCE);

		return testSpecifications.get();
	}
}
