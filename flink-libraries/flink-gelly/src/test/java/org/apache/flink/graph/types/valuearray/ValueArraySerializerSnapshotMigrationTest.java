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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * Migration tests for boxed-value array serializer snapshots.
 */
@RunWith(Parameterized.class)
public class ValueArraySerializerSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Object> {

	public ValueArraySerializerSnapshotMigrationTest(TestSpecification<Object> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"byte-value-array-serializer",
			ByteValueArraySerializer.class,
			ByteValueArraySerializer.ByteValueArraySerializerSnapshot.class,
			ByteValueArraySerializer::new);
		testSpecifications.add(
			"char-value-array-serializer",
			CharValueArraySerializer.class,
			CharValueArraySerializer.CharValueArraySerializerSnapshot.class,
			CharValueArraySerializer::new);
		testSpecifications.add(
			"double-value-array-serializer",
			DoubleValueArraySerializer.class,
			DoubleValueArraySerializer.DoubleValueArraySerializerSnapshot.class,
			DoubleValueArraySerializer::new);
		testSpecifications.add(
			"float-value-array-serializer",
			FloatValueArraySerializer.class,
			FloatValueArraySerializer.FloatValueArraySerializerSnapshot.class,
			FloatValueArraySerializer::new);
		testSpecifications.add(
			"int-value-array-serializer",
			IntValueArraySerializer.class,
			IntValueArraySerializer.IntValueArraySerializerSnapshot.class,
			IntValueArraySerializer::new);
		testSpecifications.add(
			"long-value-array-serializer",
			LongValueArraySerializer.class,
			LongValueArraySerializer.LongValueArraySerializerSnapshot.class,
			LongValueArraySerializer::new);
		testSpecifications.add(
			"null-value-array-serializer",
			NullValueArraySerializer.class,
			NullValueArraySerializer.NullValueArraySerializerSnapshot.class,
			NullValueArraySerializer::new);
		testSpecifications.add(
			"short-value-array-serializer",
			ShortValueArraySerializer.class,
			ShortValueArraySerializer.ShortValueArraySerializerSnapshot.class,
			ShortValueArraySerializer::new);
		testSpecifications.add(
			"string-value-array-serializer",
			StringValueArraySerializer.class,
			StringValueArraySerializer.StringValueArraySerializerSnapshot.class,
			StringValueArraySerializer::new);

		return testSpecifications.get();
	}
}
