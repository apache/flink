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
import org.apache.flink.api.common.typeutils.ClassRelocator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerMatchers;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerUpgradeTestBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.hamcrest.Matcher;

import java.util.Objects;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertSame;

/**
 * A collection of test specifications for the {@link PojoSerializerUpgradeTest}.
 */
public class PojoSerializerUpgradeTestSpecifications {

	// ----------------------------------------------------------------------------------------------
	//  Test types
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

	@SuppressWarnings("WeakerAccess")
	public static class StaticSchemaPojoSubclassA extends StaticSchemaPojo {

		public int subclassField;

		public StaticSchemaPojoSubclassA() {}

		public StaticSchemaPojoSubclassA(String name, int age, Color favoriteColor, boolean married, int subclassField) {
			super(name, age, favoriteColor, married);
			this.subclassField = subclassField;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}

			if (!(obj instanceof StaticSchemaPojoSubclassA)) {
				return false;
			}

			StaticSchemaPojoSubclassA other = (StaticSchemaPojoSubclassA) obj;
			return Objects.equals(name, other.name)
				&& age == other.age
				&& favoriteColor == other.favoriteColor
				&& married == other.married
				&& subclassField == other.subclassField;
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, age, favoriteColor, married, subclassField);
		}
	}

	@SuppressWarnings("WeakerAccess")
	public static class StaticSchemaPojoSubclassB extends StaticSchemaPojo {

		public boolean subclassField;

		public StaticSchemaPojoSubclassB() {}

		public StaticSchemaPojoSubclassB(String name, int age, Color favoriteColor, boolean married, boolean subclassField) {
			super(name, age, favoriteColor, married);
			this.subclassField = subclassField;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}

			if (!(obj instanceof StaticSchemaPojoSubclassB)) {
				return false;
			}

			StaticSchemaPojoSubclassB other = (StaticSchemaPojoSubclassB) obj;
			return Objects.equals(name, other.name)
				&& age == other.age
				&& favoriteColor == other.favoriteColor
				&& married == other.married
				&& subclassField == other.subclassField;
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, age, favoriteColor, married, subclassField);
		}
	}

	@SuppressWarnings("WeakerAccess")
	public static class StaticSchemaPojoSubclassC extends StaticSchemaPojo {

		public double subclassField;

		public StaticSchemaPojoSubclassC() {}

		public StaticSchemaPojoSubclassC(String name, int age, Color favoriteColor, boolean married, double subclassField) {
			super(name, age, favoriteColor, married);
			this.subclassField = subclassField;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}

			if (!(obj instanceof StaticSchemaPojoSubclassC)) {
				return false;
			}

			StaticSchemaPojoSubclassC other = (StaticSchemaPojoSubclassC) obj;
			return Objects.equals(name, other.name)
				&& age == other.age
				&& favoriteColor == other.favoriteColor
				&& married == other.married
				&& subclassField == other.subclassField;
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, age, favoriteColor, married, subclassField);
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-identical-schema"
	// ----------------------------------------------------------------------------------------------

	public static final class IdenticalPojoSchemaSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StaticSchemaPojo> {

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

	public static final class IdenticalPojoSchemaVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createUpgradedSerializer() {
			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public Matcher<StaticSchemaPojo> testDataMatcher() {
			return is(new StaticSchemaPojo("Gordon", 27, StaticSchemaPojo.Color.BLUE, false));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<StaticSchemaPojo>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAsIs();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-with-modified-schema"
	// ----------------------------------------------------------------------------------------------

	public static final class ModifiedPojoSchemaSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<ModifiedPojoSchemaSetup.PojoBeforeSchemaUpgrade> {

		@ClassRelocator.RelocateClass("TestPojoWithModifiedSchema")
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

	public static final class ModifiedPojoSchemaVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<ModifiedPojoSchemaVerifier.PojoAfterSchemaUpgrade> {

		@ClassRelocator.RelocateClass("TestPojoWithModifiedSchema")
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
		public Matcher<PojoAfterSchemaUpgrade> testDataMatcher() {
			return is(new PojoAfterSchemaUpgrade("Gordon", 27, null, false));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<PojoAfterSchemaUpgrade>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAfterMigration();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-with-different-field-types"
	// ----------------------------------------------------------------------------------------------

	public static final class DifferentFieldTypePojoSchemaSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<DifferentFieldTypePojoSchemaSetup.PojoWithIntField> {

		@ClassRelocator.RelocateClass("TestPojoWithDifferentFieldType")
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

	public static final class DifferentFieldTypePojoSchemaVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<DifferentFieldTypePojoSchemaVerifier.PojoWithStringField> {

		@ClassRelocator.RelocateClass("TestPojoWithDifferentFieldType")
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
		public Matcher<PojoWithStringField> testDataMatcher() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<PojoWithStringField>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isIncompatible();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-with-modified-schema-in-registered-subclass"
	// ----------------------------------------------------------------------------------------------

	public static final class ModifiedRegisteredPojoSubclassSchemaSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<ModifiedRegisteredPojoSubclassSchemaSetup.BasePojo> {

		@ClassRelocator.RelocateClass("BasePojo")
		public static class BasePojo {
			public int baseFieldA;
			public String baseFieldB;

			public BasePojo() {}

			public BasePojo(int baseFieldA, String baseFieldB) {
				this.baseFieldA = baseFieldA;
				this.baseFieldB = baseFieldB;
			}
		}

		@ClassRelocator.RelocateClass("SublassPojo")
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

	public static final class ModifiedRegisteredPojoSubclassSchemaVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<ModifiedRegisteredPojoSubclassSchemaVerifier.BasePojo> {

		@ClassRelocator.RelocateClass("BasePojo")
		public static class BasePojo {
			public int baseFieldA;
			public String baseFieldB;

			public BasePojo() {}

			public BasePojo(int baseFieldA, String baseFieldB) {
				this.baseFieldA = baseFieldA;
				this.baseFieldB = baseFieldB;
			}
		}

		@ClassRelocator.RelocateClass("SublassPojo")
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
		public Matcher<BasePojo> testDataMatcher() {
			return is(new SubclassPojoAfterSchemaUpgrade(911108, "Gordon", true, 0, 0.0));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<BasePojo>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleAfterMigration();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-with-different-field-types-in-registered-subclass"
	// ----------------------------------------------------------------------------------------------

	public static final class DifferentFieldTypePojoSubclassSchemaSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StaticSchemaPojo> {

		@ClassRelocator.RelocateClass("TestPojoSubclassWithDifferentFieldType")
		@SuppressWarnings("WeakerAccess")
		public static class SubclassPojoWithIntField extends StaticSchemaPojo {

			public int fieldValue;

			public SubclassPojoWithIntField() {}

			public SubclassPojoWithIntField(String name, int age, Color favoriteColor, boolean married, int fieldValue) {
				super(name, age, favoriteColor, married);
				this.fieldValue = fieldValue;
			}
		}

		@Override
		public TypeSerializer<StaticSchemaPojo> createPriorSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerPojoType(SubclassPojoWithIntField.class);

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo createTestData() {
			return new SubclassPojoWithIntField("gt", 7, StaticSchemaPojo.Color.BLUE, false, 911108);
		}
	}

	public static final class DifferentFieldTypePojoSubclassSchemaVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StaticSchemaPojo> {

		@ClassRelocator.RelocateClass("TestPojoSubclassWithDifferentFieldType")
		@SuppressWarnings("WeakerAccess")
		public static class SubclassPojoWithStringField extends StaticSchemaPojo {

			public String fieldValue;

			public SubclassPojoWithStringField() {}

			public SubclassPojoWithStringField(String name, int age, Color favoriteColor, boolean married, String fieldValue) {
				super(name, age, favoriteColor, married);
				this.fieldValue = fieldValue;
			}
		}

		@Override
		public TypeSerializer<StaticSchemaPojo> createUpgradedSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerPojoType(SubclassPojoWithStringField.class);

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public Matcher<StaticSchemaPojo> testDataMatcher() {
			return is(new SubclassPojoWithStringField("gt", 7, StaticSchemaPojo.Color.BLUE, false, "911108"));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<StaticSchemaPojo>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isIncompatible();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-with-non-registered-subclass"
	// ----------------------------------------------------------------------------------------------

	public static final class NonRegisteredPojoSubclassSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createPriorSerializer() {
			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo createTestData() {
			return new StaticSchemaPojoSubclassA("gt", 7, StaticSchemaPojo.Color.BLUE, false, 911108);
		}
	}

	public static final class NonRegisteredPojoSubclassVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createUpgradedSerializer() {
			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(new ExecutionConfig());
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public Matcher<StaticSchemaPojo> testDataMatcher() {
			return is(new StaticSchemaPojoSubclassA("gt", 7, StaticSchemaPojo.Color.BLUE, false, 911108));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<StaticSchemaPojo>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleWithReconfiguredSerializer();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-with-different-subclass-registration-order"
	// ----------------------------------------------------------------------------------------------

	public static final class DifferentPojoSubclassRegistrationOrderSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createPriorSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerPojoType(StaticSchemaPojoSubclassA.class);
			executionConfig.registerPojoType(StaticSchemaPojoSubclassB.class);

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo createTestData() {
			return new StaticSchemaPojoSubclassB("gt", 7, StaticSchemaPojo.Color.BLUE, false, true);
		}
	}

	public static final class DifferentPojoSubclassRegistrationOrderVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createUpgradedSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			// different registration order than setup
			executionConfig.registerPojoType(StaticSchemaPojoSubclassB.class);
			executionConfig.registerPojoType(StaticSchemaPojoSubclassA.class);

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public Matcher<StaticSchemaPojo> testDataMatcher() {

			return is(new StaticSchemaPojoSubclassB("gt", 7, StaticSchemaPojo.Color.BLUE, false, true));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<StaticSchemaPojo>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleWithReconfiguredSerializer();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-with-missing-registered-subclass"
	// ----------------------------------------------------------------------------------------------

	public static final class MissingRegisteredPojoSubclassSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createPriorSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerPojoType(StaticSchemaPojoSubclassA.class);
			executionConfig.registerPojoType(StaticSchemaPojoSubclassB.class);

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo createTestData() {
			return new StaticSchemaPojoSubclassB("gt", 7, StaticSchemaPojo.Color.BLUE, false, true);
		}
	}

	public static final class MissingRegisteredPojoSubclassVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createUpgradedSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			// missing registration for subclass A compared to setup
			executionConfig.registerPojoType(StaticSchemaPojoSubclassB.class);

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public Matcher<StaticSchemaPojo> testDataMatcher() {
			return is(new StaticSchemaPojoSubclassB("gt", 7, StaticSchemaPojo.Color.BLUE, false, true));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<StaticSchemaPojo>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleWithReconfiguredSerializer();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-with-new-registered-subclass"
	// ----------------------------------------------------------------------------------------------

	public static final class NewRegisteredPojoSubclassSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createPriorSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo createTestData() {
			return new StaticSchemaPojoSubclassA("gt", 7, StaticSchemaPojo.Color.BLUE, false, 911108);
		}
	}

	public static final class NewRegisteredPojoSubclassVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createUpgradedSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			// new registration for subclass A compared to setup
			executionConfig.registerPojoType(StaticSchemaPojoSubclassA.class);

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public Matcher<StaticSchemaPojo> testDataMatcher() {
			return is(new StaticSchemaPojoSubclassA("gt", 7, StaticSchemaPojo.Color.BLUE, false, 911108));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<StaticSchemaPojo>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleWithReconfiguredSerializer();
		}
	}

	// ----------------------------------------------------------------------------------------------
	//  Specification for "pojo-serializer-with-new-and-missing-registered-subclasses"
	// ----------------------------------------------------------------------------------------------

	public static final class NewAndMissingRegisteredPojoSubclassesSetup implements TypeSerializerUpgradeTestBase.PreUpgradeSetup<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createPriorSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerPojoType(StaticSchemaPojoSubclassA.class);
			executionConfig.registerPojoType(StaticSchemaPojoSubclassB.class);

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public StaticSchemaPojo createTestData() {
			return new StaticSchemaPojoSubclassB("gt", 7, StaticSchemaPojo.Color.BLUE, false, true);
		}
	}

	public static final class NewAndMissingRegisteredPojoSubclassesVerifier implements TypeSerializerUpgradeTestBase.UpgradeVerifier<StaticSchemaPojo> {

		@Override
		public TypeSerializer<StaticSchemaPojo> createUpgradedSerializer() {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerPojoType(StaticSchemaPojoSubclassB.class);
			executionConfig.registerPojoType(StaticSchemaPojoSubclassC.class);

			TypeSerializer<StaticSchemaPojo> serializer = TypeExtractor.createTypeInfo(StaticSchemaPojo.class).createSerializer(executionConfig);
			assertSame(serializer.getClass(), PojoSerializer.class);
			return serializer;
		}

		@Override
		public Matcher<StaticSchemaPojo> testDataMatcher() {
			return is(new StaticSchemaPojoSubclassB("gt", 7, StaticSchemaPojo.Color.BLUE, false, true));
		}

		@Override
		public Matcher<TypeSerializerSchemaCompatibility<StaticSchemaPojo>> schemaCompatibilityMatcher(MigrationVersion version) {
			return TypeSerializerMatchers.isCompatibleWithReconfiguredSerializer();
		}
	}
}
