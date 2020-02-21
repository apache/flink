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

package org.apache.flink.table.dataview;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link ListViewSerializerSnapshot}.
 */
@RunWith(Parameterized.class)
public class ListViewSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<ListView<String>, ListView<String>> {

	private static final String SPEC_NAME = "list-view-serializer";

	public ListViewSerializerUpgradeTest(TestSpecification<ListView<String>, ListView<String>> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
			testSpecifications.add(
				new TestSpecification<>(
					SPEC_NAME,
					migrationVersion,
					ListViewSerializerSetup.class,
					ListViewSerializerVerifier.class));
		}
		return testSpecifications;
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "list-view-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class ListViewSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<ListView<String>> {
		@Override
		public TypeSerializer<ListView<String>> createPriorSerializer() {
			return new ListViewSerializer<>(new ListSerializer<>(StringSerializer.INSTANCE));
		}

		@Override
		public ListView<String> createTestData() {
			return mockTestData();
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class ListViewSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<ListView<String>> {
		@Override
		public TypeSerializer<ListView<String>> createUpgradedSerializer() {
			return new ListViewSerializer<>(new ListSerializer<>(StringSerializer.INSTANCE));
		}

		@Override
		public Matcher<ListView<String>> testDataMatcher() {
			// ListViewSerializer will not (de)serialize the TypeInformation, so we have to create
			// a ListView without TypeInformation for comparison
			return is(mockTestDataWithoutTypeInfo());
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<ListView<String>>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	public static ListView<String> mockTestData() {
		ListView<String> view = new ListView<>(TypeInformation.of(String.class));
		try {
			view.add("ApacheFlink");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return view;
	}

	public static ListView<String> mockTestDataWithoutTypeInfo() {
		ListView<String> view = new ListView<>();
		try {
			view.add("ApacheFlink");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return view;
	}
}
