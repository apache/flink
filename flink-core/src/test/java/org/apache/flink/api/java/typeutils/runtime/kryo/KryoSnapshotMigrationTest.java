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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Animal;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Cat;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Dog;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.DogV2KryoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Parrot;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.ParrotKryoSerializer;
import org.apache.flink.testutils.migration.MigrationVersion;

import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility.compatibleAsIs;
import static org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer;

/**
 * Tests migrations for {@link KryoSerializerSnapshot}.
 */
@SuppressWarnings("WeakerAccess")
@RunWith(Parameterized.class)
public class KryoSnapshotMigrationTest extends TypeSerializerSnapshotMigrationTestBase<KryoPojosForMigrationTests> {

	public KryoSnapshotMigrationTest(TestSpecification<KryoPojosForMigrationTests> testSpecification) {
		super(testSpecification);
	}

	@SuppressWarnings("unchecked")
	@Parameterized.Parameters(name = "Test Specification = {0}")
	public static Collection<Object[]> testSpecifications() {

		List<Object[]> specs = new ArrayList<>();

		add(specs, "kryo-type-serializer-empty-config",
			() -> new KryoSerializer<>(Animal.class, new ExecutionConfig()));

		add(specs, "kryo-type-serializer-empty-config", () -> {

			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerKryoType(DummyClassOne.class);
			executionConfig.registerTypeWithKryoSerializer(DummyClassTwo.class, StringSerializer.class);

			return new KryoSerializer<>(Animal.class, executionConfig);
		}, COMPATIBLE_WITH_RECONFIGURED);

		add(specs, "kryo-type-serializer", () -> {

			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerKryoType(DummyClassOne.class);
			executionConfig.registerKryoType(Dog.class);
			executionConfig.registerKryoType(DummyClassTwo.class);
			executionConfig.registerKryoType(Cat.class);
			executionConfig.registerKryoType(Parrot.class);

			return new KryoSerializer<>(Animal.class, executionConfig);
		}, COMPATIBLE_WITH_RECONFIGURED);

		add(specs, "kryo-type-serializer-custom", () -> {
			ExecutionConfig executionConfig = new ExecutionConfig();
			executionConfig.registerKryoType(DummyClassOne.class);
			executionConfig.registerTypeWithKryoSerializer(Dog.class, DogV2KryoSerializer.class);
			executionConfig.registerKryoType(DummyClassTwo.class);
			executionConfig.registerKryoType(Cat.class);
			executionConfig.registerTypeWithKryoSerializer(Parrot.class, ParrotKryoSerializer.class);

			return new KryoSerializer<>(Animal.class, executionConfig);
		}, COMPATIBLE_WITH_RECONFIGURED);

		return specs;
	}

	private static void add(List<Object[]> all, String name, Supplier<TypeSerializer<Animal>> supplier) {
		add(all, name, supplier, compatibleAsIs());
	}

	private static void add(List<Object[]> all,
							String name, Supplier<TypeSerializer<Animal>> supplier,
							TypeSerializerSchemaCompatibility<Animal> expected) {

		TestSpecification<Animal> flink16 = TestSpecification.<Animal>builder(
			MigrationVersion.v1_6 + " " + name,
			KryoSerializer.class,
			KryoSerializerSnapshot.class,
			MigrationVersion.v1_6)
			.withNewSerializerProvider(supplier, expected)
			.withSnapshotDataLocation("flink-1.6-" + name + "-snapshot")
			.withTestData("flink-1.6-" + name + "-data", 2);

		TestSpecification<Animal> flink17 = TestSpecification.<Animal>builder(
			MigrationVersion.v1_7 + " " + name,
			KryoSerializer.class,
			KryoSerializerSnapshot.class,
			MigrationVersion.v1_7)
			.withNewSerializerProvider(supplier, expected)
			.withSnapshotDataLocation("flink-1.7-" + name + "-snapshot")
			.withTestData("flink-1.7-" + name + "-data", 2);

		all.add(new Object[]{flink16});
		all.add(new Object[]{flink17});
	}

	/**
	 * Dummy class to be registered in the tests.
	 */
	public static final class DummyClassOne {

	}

	/**
	 * Dummy class to be registered in the tests.
	 */
	public static final class DummyClassTwo {

	}

	private static final TypeSerializerSchemaCompatibility<Animal> COMPATIBLE_WITH_RECONFIGURED =
		compatibleWithReconfiguredSerializer(new KryoSerializer<>(Animal.class, new ExecutionConfig()));
}
