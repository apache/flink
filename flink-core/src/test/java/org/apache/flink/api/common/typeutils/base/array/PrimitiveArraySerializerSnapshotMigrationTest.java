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

package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * Migration tests for primitive array type serializers' snapshots.
 */
@RunWith(Parameterized.class)
public class PrimitiveArraySerializerSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Object> {

	public PrimitiveArraySerializerSnapshotMigrationTest(TestSpecification<Object> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"boolean-primitive-array-serializer",
			BooleanPrimitiveArraySerializer.class,
			BooleanPrimitiveArraySerializer.BooleanPrimitiveArraySerializerSnapshot.class,
			() -> BooleanPrimitiveArraySerializer.INSTANCE);
		testSpecifications.add(
			"byte-primitive-array-serializer",
			BytePrimitiveArraySerializer.class,
			BytePrimitiveArraySerializer.BytePrimitiveArraySerializerSnapshot.class,
			() -> BytePrimitiveArraySerializer.INSTANCE);
		testSpecifications.add(
			"char-primitive-array-serializer",
			CharPrimitiveArraySerializer.class,
			CharPrimitiveArraySerializer.CharPrimitiveArraySerializerSnapshot.class,
			() -> CharPrimitiveArraySerializer.INSTANCE);
		testSpecifications.add(
			"double-primitive-array-serializer",
			DoublePrimitiveArraySerializer.class,
			DoublePrimitiveArraySerializer.DoublePrimitiveArraySerializerSnapshot.class,
			() -> DoublePrimitiveArraySerializer.INSTANCE);
		testSpecifications.add(
			"float-primitive-array-serializer",
			FloatPrimitiveArraySerializer.class,
			FloatPrimitiveArraySerializer.FloatPrimitiveArraySerializerSnapshot.class,
			() -> FloatPrimitiveArraySerializer.INSTANCE);
		testSpecifications.add(
			"int-primitive-array-serializer",
			IntPrimitiveArraySerializer.class,
			IntPrimitiveArraySerializer.IntPrimitiveArraySerializerSnapshot.class,
			() -> IntPrimitiveArraySerializer.INSTANCE);
		testSpecifications.add(
			"long-primitive-array-serializer",
			LongPrimitiveArraySerializer.class,
			LongPrimitiveArraySerializer.LongPrimitiveArraySerializerSnapshot.class,
			() -> LongPrimitiveArraySerializer.INSTANCE);
		testSpecifications.add(
			"short-primitive-array-serializer",
			ShortPrimitiveArraySerializer.class,
			ShortPrimitiveArraySerializer.ShortPrimitiveArraySerializerSnapshot.class,
			() -> ShortPrimitiveArraySerializer.INSTANCE);
		testSpecifications.add(
			"string-array-serializer",
			StringArraySerializer.class,
			StringArraySerializer.StringArraySerializerSnapshot.class,
			() -> StringArraySerializer.INSTANCE);

		return testSpecifications.get();
	}

}
