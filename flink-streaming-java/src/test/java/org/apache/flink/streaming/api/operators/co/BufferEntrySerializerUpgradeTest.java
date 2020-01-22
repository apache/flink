/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.operators.co.IntervalJoinOperator.BufferEntry;
import org.apache.flink.streaming.api.operators.co.IntervalJoinOperator.BufferEntrySerializer;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.streaming.api.operators.co.BufferEntryMatchers.bufferEntry;
import static org.hamcrest.Matchers.is;

/**
 * State migration tests for {@link BufferEntrySerializer}.
 */
@RunWith(Parameterized.class)
public class BufferEntrySerializerUpgradeTest
		extends TypeSerializerUpgradeTestBase<BufferEntry<String>, BufferEntry<String>> {

	public BufferEntrySerializerUpgradeTest(
			TestSpecification<BufferEntry<String>, BufferEntry<String>> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
			testSpecifications.add(
					new TestSpecification<>(
							"buffer-entry-serializer",
							migrationVersion,
							BufferEntrySerializerSetup.class,
							BufferEntrySerializerVerifier.class));
		}

		return testSpecifications;
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "buffer-entry-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class BufferEntrySerializerSetup
			implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<BufferEntry<String>> {

		@SuppressWarnings({"unchecked", "rawtypes"})
		@Override
		public TypeSerializer<BufferEntry<String>> createPriorSerializer() {
			return new BufferEntrySerializer(StringSerializer.INSTANCE);
		}

		@Override
		public BufferEntry<String> createTestData() {
			return new BufferEntry<>("hello", false);
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class BufferEntrySerializerVerifier
			implements TypeSerializerUpgradeTestBase.UpgradeVerifier<BufferEntry<String>> {

		@SuppressWarnings({"unchecked", "rawtypes"})
		@Override
		public TypeSerializer<BufferEntry<String>> createUpgradedSerializer() {
			return new BufferEntrySerializer(StringSerializer.INSTANCE);
		}

		@Override
		public Matcher<BufferEntry<String>> testDataMatcher() {
			return bufferEntry(is("hello"), is(false));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<BufferEntry<String>>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
}
