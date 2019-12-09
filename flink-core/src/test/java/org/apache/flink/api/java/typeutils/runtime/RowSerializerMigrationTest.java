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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer.RowSerializerSnapshot;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.types.Row;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

/**
 * State migration test for {@link RowSerializer}.
 */
@RunWith(Parameterized.class)
public class RowSerializerMigrationTest extends TypeSerializerSnapshotMigrationTestBase<Row> {

	public RowSerializerMigrationTest(TestSpecification<Row> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?>> testSpecifications() {

		final TestSpecifications testSpecifications = new TestSpecifications(MigrationVersion.v1_6, MigrationVersion.v1_7);

		testSpecifications.add(
			"row-serializer",
			RowSerializer.class,
			RowSerializerSnapshot.class,
			RowSerializerMigrationTest::stringLongRowSupplier);

		return testSpecifications.get();
	}

	private static TypeSerializer<Row> stringLongRowSupplier() {
		RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
		return rowTypeInfo.createSerializer(new ExecutionConfig());
	}
}
