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
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.hamcrest.Matcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import static org.apache.flink.api.common.typeutils.ClassRelocator.RelocateClass;
import static org.junit.Assert.assertSame;

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
		return Arrays.asList(
			new TestSpecification<>(
				"pojo-serializer-identical-schema",
				MigrationVersion.v1_7,
				IdenticalPojoSchemaSetup.class,
				IdenticalPojoSchemaVerifier.class),
			new TestSpecification<>(
				"pojo-serializer-with-modified-schema",
				MigrationVersion.v1_7,
				ModifiedPojoSchemaSetup.class,
				ModifiedPojoSchemaVerifier.class),
			new TestSpecification<>(
				"pojo-serializer-with-different-field-types",
				MigrationVersion.v1_7,
				DifferentFieldTypePojoSchemaSetup.class,
 				DifferentFieldTypePojoSchemaVerifier.class),
			new TestSpecification<>(
				"pojo-serializer-with-modified-schema-in-registered-subclass",
				MigrationVersion.v1_7,
				ModifiedRegisteredPojoSubclassSchemaSetup.class,
				ModifiedRegisteredPojoSubclassSchemaVerifier.class)
			/*
			new TestSpecification<>(
				"pojo-serializer-with-different-field-type-in-registered-subclass",
				MigrationVersion.v1_7,
				DifferentFieldTypePojoSchemaSetup.class,
				DifferentFieldTypePojoSchemaVerifier.class),
			new TestSpecification<>(
				"pojo-serializer-with-non-registered-subclass",
				MigrationVersion.v1_7,
				DifferentFieldTypePojoSchemaSetup.class,
				DifferentFieldTypePojoSchemaVerifier.class),
			new TestSpecification<>(
				"pojo-serializer-with-different-subclass-registration-order",
				MigrationVersion.v1_7,
				DifferentFieldTypePojoSchemaSetup.class,
				DifferentFieldTypePojoSchemaVerifier.class),
			new TestSpecification<>(
				"pojo-serializer-with-missing-registered-subclass",
				MigrationVersion.v1_7,
				DifferentFieldTypePojoSchemaSetup.class,
				DifferentFieldTypePojoSchemaVerifier.class),
			new TestSpecification<>(
				"pojo-serializer-with-missing-non-registered-subclass",
				MigrationVersion.v1_7,
				DifferentFieldTypePojoSchemaSetup.class,
				DifferentFieldTypePojoSchemaVerifier.class)
				*/
		);
	}

	// ----------------------------------------------------------------------------------------------
	//  Test specification setup & verifiers
	// ----------------------------------------------------------------------------------------------

	@SuppressWarnings("WeakerAccess")
	public static class StaticSchemaPojo {

		public enum Color {
			RED, BLUE, GREEN
		}

		public String name;
		public int age;
		public Color favoriteColor;
		public boolean married;

		public StaticSchemaPojo() {}

		public StaticSchemaPojo(String name, int age, Color favoriteColor, boolean married) {
			this.name = name;
			this.age = age;
			this.favoriteColor = favoriteColor;
			this.married = married;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}

			if (!(obj instanceof StaticSchemaPojo)) {
				return false;
			}

			StaticSchemaPojo other = (StaticSchemaPojo) obj;
			return Objects.equals(name, other.name)
				&& age == other.age
				&& favoriteColor == other.favoriteColor
				&& married == other.married;
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, age, favoriteColor, married);
		}
	}

	public static final class IdenticalPojoSchemaSetup implements PreUpgradeSetup<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createPriorSerializer() {
			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo createTestData() {
			return new StaticSchemaPojo("Gordon", 27, StaticSchemaPojo.Color.BLUE, false);
		}
	}

	public static final class IdenticalPojoSchemaVerifier implements UpgradeVerifier<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createUpgradedSerializer() {
			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo expectedTestData() {
			return new StaticSchemaPojo("Gordon", 27, StaticSchemaPojo.Color.BLUE, false);
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<StaticSchemaPojo>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	public static final class ModifiedPojoSchemaSetup implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {

		@RelocateClass("TestPojoWithModifiedSchema")
		@SuppressWarnings("WeakerAccess")
		public static class PojoBeforeSchemaUpgrade {
			public int id;
			public String name;
			public int age;
			public double height;

			public PojoBeforeSchemaUpgrade() {}

			public PojoBeforeSchemaUpgrade(int id, String name, int age, double height) {
				this.id = id;
				this.name = name;
				this.age = age;
				this.height = height;
			}
		}

		@Override
		public TypeSerializer<PojoBeforeSchemaUpgrade> createPriorSerializer() {
			TypeSerializer<PojoBeforeSchemaUpgrade> serializer = TypeExtractor.createTypeInfo(PojoBeforeSchemaUpgrade.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public PojoBeforeSchemaUpgrade createTestData() {
			return new PojoBeforeSchemaUpgrade(911108, "Gordon", 27, 172.8);
		}
	}

	public static final class ModifiedPojoSchemaVerifier implements UpgradeVerifier<ModifiedPojoSchemaVerifier.PojoAfterSchemaUpgrade> {

		@RelocateClass("TestPojoWithModifiedSchema")
		@SuppressWarnings("WeakerAccess")
		public static class PojoAfterSchemaUpgrade {

			public enum Color {
				RED, BLUE, GREEN
			}

			public String name;
			public int age;
			public Color favoriteColor;
			public boolean married;

			public PojoAfterSchemaUpgrade() {}

			public PojoAfterSchemaUpgrade(String name, int age, Color favoriteColor, boolean married) {
				this.name = name;
				this.age = age;
				this.favoriteColor = favoriteColor;
				this.married = married;
			}

			@Override
			public boolean equals(Object obj) {
				if (obj == null) {
					return false;
				}

				if (!(obj instanceof PojoAfterSchemaUpgrade)) {
					return false;
				}

				PojoAfterSchemaUpgrade other = (PojoAfterSchemaUpgrade) obj;
				return Objects.equals(other.name, name)
					&& other.age == age
					&& other.favoriteColor == favoriteColor
					&& other.married == married;
			}

			@Override
			public int hashCode() {
				return Objects.hash(name, age, favoriteColor, married);
			}
		}

		@Override
		public TypeSerializer<PojoAfterSchemaUpgrade> createUpgradedSerializer() {
			TypeSerializer<PojoAfterSchemaUpgrade> serializer = TypeExtractor.createTypeInfo(PojoAfterSchemaUpgrade.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public PojoAfterSchemaUpgrade expectedTestData() {
			return new PojoAfterSchemaUpgrade("Gordon", 27, null, false);
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<PojoAfterSchemaUpgrade>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAfterMigration();
		}
	}

	public static final class DifferentFieldTypePojoSchemaSetup implements PreUpgradeSetup<DifferentFieldTypePojoSchemaSetup.PojoWithIntField> {

		@RelocateClass("TestPojoWithDifferentFieldType")
		@SuppressWarnings("WeakerAccess")
		public static class PojoWithIntField {

			public int fieldValue;

			public PojoWithIntField() {}

			public PojoWithIntField(int fieldValue) {
				this.fieldValue = fieldValue;
			}
		}

		@Override
		public TypeSerializer<PojoWithIntField> createPriorSerializer() {
			TypeSerializer<PojoWithIntField> serializer = TypeExtractor.createTypeInfo(PojoWithIntField.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public PojoWithIntField createTestData() {
			return new PojoWithIntField(911108);
		}
	}

	public static final class DifferentFieldTypePojoSchemaVerifier implements UpgradeVerifier<DifferentFieldTypePojoSchemaVerifier.PojoWithStringField> {

		@RelocateClass("TestPojoWithDifferentFieldType")
		@SuppressWarnings("WeakerAccess")
		public static class PojoWithStringField {

			public String fieldValue;

			public PojoWithStringField() {}

			public PojoWithStringField(String fieldValue) {
				this.fieldValue = fieldValue;
			}
		}

		@Override
		public TypeSerializer<PojoWithStringField> createUpgradedSerializer() {
			TypeSerializer<PojoWithStringField> serializer = TypeExtractor.createTypeInfo(PojoWithStringField.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public PojoWithStringField expectedTestData() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<PojoWithStringField>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isIncompatible();
		}
	}

	public static final class ModifiedRegisteredPojoSubclassSchemaSetup implements PreUpgradeSetup<ModifiedRegisteredPojoSubclassSchemaSetup.BasePojo> {

		@RelocateClass("BasePojo")
		public static class BasePojo {
			public int baseFieldA;
			public String baseFieldB;

			public BasePojo() {}

			public BasePojo(int baseFieldA, String baseFieldB) {
				this.baseFieldA = baseFieldA;
				this.baseFieldB = baseFieldB;
			}
		}

		@RelocateClass("SublassPojo")
		public static class SubclassPojoBeforeSchemaUpgrade extends BasePojo {
			public boolean subclassFieldC;
			public double subclassFieldD;

			public SubclassPojoBeforeSchemaUpgrade() {}

			public SubclassPojoBeforeSchemaUpgrade(int baseFieldA, String baseFieldB, boolean subclassFieldC, double subclassFieldD) {
				super(baseFieldA, baseFieldB);
				this.subclassFieldC = subclassFieldC;
				this.subclassFieldD = subclassFieldD;
			}
		}

		@Override
		public TypeSerializer<BasePojo> createPriorSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerPojoType(SubclassPojoBeforeSchemaUpgrade.class);

			TypeSerializer<BasePojo> serializer = TypeExtractor.createTypeInfo(BasePojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public BasePojo createTestData() {
			return new SubclassPojoBeforeSchemaUpgrade(911108, "Gordon", true, 66.7);
		}
	}

	public static final class ModifiedRegisteredPojoSubclassSchemaVerifier implements UpgradeVerifier<ModifiedRegisteredPojoSubclassSchemaVerifier.BasePojo> {

		@RelocateClass("BasePojo")
		public static class BasePojo {
			public int baseFieldA;
			public String baseFieldB;

			public BasePojo() {}

			public BasePojo(int baseFieldA, String baseFieldB) {
				this.baseFieldA = baseFieldA;
				this.baseFieldB = baseFieldB;
			}
		}

		@RelocateClass("SublassPojo")
		public static class SubclassPojoAfterSchemaUpgrade extends BasePojo {
			public boolean subclassFieldC;
			public long subclassFieldE;
			public double subclassFieldF;

			public SubclassPojoAfterSchemaUpgrade() {}

			public SubclassPojoAfterSchemaUpgrade(int baseFieldA, String baseFieldB, boolean subclassFieldC, long subclassFieldE, double subclassFieldF) {
				super(baseFieldA, baseFieldB);
				this.subclassFieldC = subclassFieldC;
				this.subclassFieldE = subclassFieldE;
				this.subclassFieldF = subclassFieldF;
			}

			@Override
			public boolean equals(Object obj) {
				if (obj == null) {
					return false;
				}

				if (!(obj instanceof SubclassPojoAfterSchemaUpgrade)) {
					return false;
				}

				SubclassPojoAfterSchemaUpgrade other = (SubclassPojoAfterSchemaUpgrade) obj;
				return other.baseFieldA == baseFieldA
					&& Objects.equals(other.baseFieldB, baseFieldB)
					&& other.subclassFieldC == subclassFieldC
					&& other.subclassFieldE == subclassFieldE
					&& other.subclassFieldF == subclassFieldF;
			}

			@Override
			public int hashCode() {
				return Objects.hash(baseFieldA, baseFieldB, subclassFieldC, subclassFieldE, subclassFieldF);
			}
		}

		@Override
		public TypeSerializer<BasePojo> createUpgradedSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerPojoType(SubclassPojoAfterSchemaUpgrade.class);

			TypeSerializer<BasePojo> serializer = TypeExtractor.createTypeInfo(BasePojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public BasePojo expectedTestData() {
			return new SubclassPojoAfterSchemaUpgrade(911108, "Gordon", true, 0, 0.0);
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<BasePojo>> schemaCompatibilityMatcher() {
			return TypeSerializerMatchers.isCompatibleAfterMigration();
		}
	}

	/*
	public static final class DifferentFieldTypePojoSubclassSchemaSetup implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}

	public static final class DifferentFieldTypePojoSubclassSchemaVerifier implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}

	public static final class NonRegisteredPojoSubclassSetup implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}

	public static final class NonRegisteredPojoSubclassVerifier implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}

	public static final class DifferentPojoSubclassRegistrationOrderSetup implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}

	public static final class DifferentPojoSubclassRegistrationOrderVerifier implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}

	public static final class MissingRegisteredPojoSubclassSetup implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}

	public static final class MissingRegisteredPojoSubclassVerifier implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}

	public static final class MissingNonRegisteredPojoSubclassSetup implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}

	public static final class MissingNonRegisteredPojoSubclassSetup implements PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {}
	*/
}
