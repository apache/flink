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

import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A {@link TypeSerializerUpgradeTestBase} for the {@link PojoSerializer}.
 */
@RunWith(Parameterized.class)
public class PojoSerializerUpgradeTest extends TypeSerializerUpgradeTestBase<Object, Object> {

	public PojoSerializerUpgradeTest(TestSpecification<Object, Object> testSpecification) {
		super(testSpecification);
	}

	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<TestSpecification<?, ?>> testSpecifications() throws Exception {
		ArrayList<TestSpecification<?, ?>> testSpecifications = new ArrayList<>();
		for (MigrationVersion migrationVersion : migrationVersions) {
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-identical-schema",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.IdenticalPojoSchemaSetup.class,
					PojoSerializerUpgradeTestSpecifications.IdenticalPojoSchemaVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-with-modified-schema",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.ModifiedPojoSchemaSetup.class,
					PojoSerializerUpgradeTestSpecifications.ModifiedPojoSchemaVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-with-different-field-types",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.DifferentFieldTypePojoSchemaSetup.class,
					PojoSerializerUpgradeTestSpecifications.DifferentFieldTypePojoSchemaVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-with-modified-schema-in-registered-subclass",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.ModifiedRegisteredPojoSubclassSchemaSetup.class,
					PojoSerializerUpgradeTestSpecifications.ModifiedRegisteredPojoSubclassSchemaVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-with-different-field-types-in-registered-subclass",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.DifferentFieldTypePojoSubclassSchemaSetup.class,
					PojoSerializerUpgradeTestSpecifications.DifferentFieldTypePojoSubclassSchemaVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-with-non-registered-subclass",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.NonRegisteredPojoSubclassSetup.class,
					PojoSerializerUpgradeTestSpecifications.NonRegisteredPojoSubclassVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-with-different-subclass-registration-order",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.DifferentPojoSubclassRegistrationOrderSetup.class,
					PojoSerializerUpgradeTestSpecifications.DifferentPojoSubclassRegistrationOrderVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-with-missing-registered-subclass",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.MissingRegisteredPojoSubclassSetup.class,
					PojoSerializerUpgradeTestSpecifications.MissingRegisteredPojoSubclassVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-with-new-registered-subclass",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.NewRegisteredPojoSubclassSetup.class,
					PojoSerializerUpgradeTestSpecifications.NewRegisteredPojoSubclassVerifier.class));
			testSpecifications.add(
				new TestSpecification<>(
					"pojo-serializer-with-new-and-missing-registered-subclasses",
					migrationVersion,
					PojoSerializerUpgradeTestSpecifications.NewAndMissingRegisteredPojoSubclassesSetup.class,
					PojoSerializerUpgradeTestSpecifications.NewAndMissingRegisteredPojoSubclassesVerifier.class));
		}

		return testSpecifications;
	}
}
