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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link Lockable.LockableTypeSerializer}.
 */
@RunWith(Parameterized.class)
public class LockableTypeSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Lockable<String>, Lockable<String>> {

	private static final String SPEC_NAME = "lockable-type-serializer";

	public LockableTypeSerializerUpgradeTest(TestSpecification<Lockable<String>, Lockable<String>> testSpecification) {
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
					LockableTypeSerializerSetup.class,
					LockableTypeSerializerVerifier.class));
		}
		return testSpecifications;
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "lockabletype-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class LockableTypeSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Lockable<String>> {
		@Override
		public TypeSerializer<Lockable<String>> createPriorSerializer() {
			return new Lockable.LockableTypeSerializer<>(StringSerializer.INSTANCE);
		}

		@Override
		public Lockable<String> createTestData() {
			return new Lockable<>("flink", 10);
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class LockableTypeSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Lockable<String>> {
		@Override
		public TypeSerializer<Lockable<String>> createUpgradedSerializer() {
			return new Lockable.LockableTypeSerializer<>(StringSerializer.INSTANCE);
		}

		@Override
		public Matcher<Lockable<String>> testDataMatcher() {
			return is(new Lockable<>("flink", 10));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<Lockable<String>>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
}
