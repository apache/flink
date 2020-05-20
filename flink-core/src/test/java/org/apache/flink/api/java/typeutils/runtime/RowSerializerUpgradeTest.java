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
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.types.Row;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link RowSerializer}.
 */
@RunWith(Parameterized.class)
public class RowSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Row, Row> {

	public RowSerializerUpgradeTest(TestSpecification<Row, Row> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {
		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
			testSpecifications.add(
				new TestSpecification<>(
					"row-serializer",
					migrationVersion,
					RowSerializerSetup.class,
					RowSerializerVerifier.class));
		}
		return testSpecifications;
	}

	public static TypeSerializer<Row> stringLongRowSupplier() {
		RowTypeInfo rowTypeInfo = new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
		return rowTypeInfo.createSerializer(new ExecutionConfig());
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "row-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class RowSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Row> {
		@Override
		public TypeSerializer<Row> createPriorSerializer() {
			return stringLongRowSupplier();
		}

		@Override
		public Row createTestData() {
			Row row = new Row(2);
			row.setField(0, "flink");
			row.setField(1, 42L);
			return row;
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class RowSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Row> {
		@Override
		public TypeSerializer<Row> createUpgradedSerializer() {
			return stringLongRowSupplier();
		}

		@Override
		public Matcher<Row> testDataMatcher() {
			Row row = new Row(2);
			row.setField(0, "flink");
			row.setField(1, 42L);
			return is(row);
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<Row>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
}
