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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.EitherSerializer;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.types.Either;

import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.is;

/**
 * A {@link TypeSerializerUpgradeTestBase} for {@link GenericArraySerializer}.
 */
@RunWith(Parameterized.class)
public class CompositeTypeSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

	public CompositeTypeSerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {

		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : MIGRATION_VERSIONS) {
			testSpecifications.add(
				new TestSpecification<>(
					"either-serializer",
					migrationVersion,
					EitherSerializerSetup.class,
					EitherSerializerVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"generic-array-serializer",
					migrationVersion,
					GenericArraySerializerSetup.class,
					GenericArraySerializerVerifier.class));
		}
		return testSpecifications;
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "either-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class EitherSerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<Either<String, Integer>> {
		@Override
		public TypeSerializer<Either<String, Integer>> createPriorSerializer() {
			return new EitherSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE);
		}

		@Override
		public Either<String, Integer> createTestData() {
			return new Either.Left<>("ApacheFlink");
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class EitherSerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<Either<String, Integer>> {
		@Override
		public TypeSerializer<Either<String, Integer>> createUpgradedSerializer() {
			return new EitherSerializer<>(StringSerializer.INSTANCE, IntSerializer.INSTANCE);
		}

		@Override
		public Matcher<Either<String, Integer>> testDataMatcher() {
			return is(new Either.Left<>("ApacheFlink"));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<Either<String, Integer>>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "generic-array-serializer"
	// ----------------------------------------------------------------------------------------------

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class GenericArraySerializerSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<String[]> {
		@Override
		public TypeSerializer<String[]> createPriorSerializer() {
			return new GenericArraySerializer<>(String.class, StringSerializer.INSTANCE);
		}

		@Override
		public String[] createTestData() {
			String[] data = {"Apache", "Flink"};
			return data;
		}
	}

	/**
	 * This class is only public to work with {@link org.apache.flink.api.common.typeutils.ClassRelocator}.
	 */
	public static final class GenericArraySerializerVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<String[]> {
		@Override
		public TypeSerializer<String[]> createUpgradedSerializer() {
			return new GenericArraySerializer<>(String.class, StringSerializer.INSTANCE);
		}

		@Override
		public Matcher<String[]> testDataMatcher() {
			String[] data = {"Apache", "Flink"};
			return is(data);
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<String[]>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}
}
