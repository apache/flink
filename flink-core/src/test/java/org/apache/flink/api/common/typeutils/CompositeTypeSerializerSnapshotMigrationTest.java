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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializerSnapshot;
import org.apache.flink.types.Either;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Migration tests for Composite Types.
 */
@RunWith(Parameterized.class)
public class CompositeTypeSerializerSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Object> {

	public CompositeTypeSerializerSnapshotMigrationTest(TestSpecification<Object> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<Object[]> testSpecifications() {

		// Either<String, Integer>

		final TestSpecification<Either<String, Integer>> either = TestSpecification.<Either<String, Integer>>builder("1.6-either", EitherSerializer.class, EitherSerializerSnapshot.class)
			.withSerializerProvider(() -> new EitherSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE))
			.withSnapshotDataLocation("flink-1.6-either-type-serializer-snapshot")
			.withTestData("flink-1.6-either-type-serializer-data", 10);

		// GenericArray<String>

		final TestSpecification<String[]> array = TestSpecification.<String[]>builder("1.6-generic-array", GenericArraySerializer.class, GenericArraySerializerConfigSnapshot.class)
			.withSerializerProvider(() -> new GenericArraySerializer<>(String.class, StringSerializer.INSTANCE))
			.withSnapshotDataLocation("flink-1.6-array-type-serializer-snapshot")
			.withTestData("flink-1.6-array-type-serializer-data", 10);

		return Arrays.asList(
			new Object[]{either},
			new Object[]{array}
		);
	}
}
