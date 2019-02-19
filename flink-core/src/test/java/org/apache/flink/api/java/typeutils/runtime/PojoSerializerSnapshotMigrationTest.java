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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * Migration tests for the {@link PojoSerializerSnapshot}.
 */
@RunWith(Parameterized.class)
public class PojoSerializerSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<PojoSerializerSnapshotMigrationTest.TestPojo> {

	public static class TestPojo {
		public int id;
		public String name;
		public int age;

		public TestPojo() {}

		public TestPojo(int id, String name, int age) {
			this.id = id;
			this.name = name;
			this.age = age;
		}
	}

	public PojoSerializerSnapshotMigrationTest(
		TestSpecification<TestPojo> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"pojo-serializer",
			PojoSerializer.class,
			PojoSerializerSnapshot.class,
			PojoSerializerSnapshotMigrationTest::testPojoSerializerSupplier);

		return testSpecifications.get();
	}

	private static TypeSerializer<TestPojo> testPojoSerializerSupplier() {
		TypeSerializer<TestPojo> serializer = TypeExtractor.createTypeInfo(TestPojo.class).createSerializer(new ExecutionConfig());
		assertTrue(serializer instanceof PojoSerializer);
		return serializer;
	}
}
