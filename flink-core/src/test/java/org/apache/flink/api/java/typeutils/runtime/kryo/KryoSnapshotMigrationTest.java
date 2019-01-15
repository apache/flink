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
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotMigrationTestBase;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Animal;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Cat;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Dog;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Parrot;
import org.apache.flink.testutils.migration.MigrationVersion;

import com.esotericsoftware.kryo.serializers.DefaultSerializers.StringSerializer;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

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

		final TestSpecification<Animal> genericCase = TestSpecification.<Animal>builder("1.6-kryo: empty config -> empty config", KryoSerializer.class, KryoSerializerSnapshot.class, MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> new KryoSerializer<>(Animal.class, new ExecutionConfig()))
			.withSnapshotDataLocation("flink-1.6-kryo-type-serializer-empty-config-snapshot")
			.withTestData("flink-1.6-kryo-type-serializer-empty-config-data", 2);

		TypeSerializerSchemaCompatibility<Animal> compatibleWithReconfigured =
			compatibleWithReconfiguredSerializer(new KryoSerializer<>(Animal.class, new ExecutionConfig()));

		final TestSpecification<Animal> additionalClasses = TestSpecification.<Animal>builder("1.6-kryo: empty config -> new classes", KryoSerializer.class, KryoSerializerSnapshot.class, MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> {
				ExecutionConfig executionConfig = new ExecutionConfig();
				executionConfig.registerKryoType(DummyClassOne.class);
				executionConfig.registerTypeWithKryoSerializer(DummyClassTwo.class, StringSerializer.class);

				return new KryoSerializer<>(Animal.class, executionConfig);
			}, compatibleWithReconfigured)
			.withSnapshotDataLocation("flink-1.6-kryo-type-serializer-empty-config-snapshot")
			.withTestData("flink-1.6-kryo-type-serializer-empty-config-data", 2);

		final TestSpecification<Animal> differentOrder = TestSpecification.<Animal>builder("1.6-kryo: registered classes in a different order", KryoSerializer.class, KryoSerializerSnapshot.class, MigrationVersion.v1_6)
			.withNewSerializerProvider(() -> {

				ExecutionConfig executionConfig = new ExecutionConfig();
				executionConfig.registerKryoType(DummyClassOne.class);
				executionConfig.registerKryoType(Dog.class);
				executionConfig.registerKryoType(DummyClassTwo.class);
				executionConfig.registerKryoType(Cat.class);
				executionConfig.registerKryoType(Parrot.class);

				return new KryoSerializer<>(Animal.class, executionConfig);
			}, compatibleWithReconfigured)
			.withSnapshotDataLocation("flink-1.6-kryo-type-serializer-snapshot")
			.withTestData("flink-1.6-kryo-type-serializer-data", 2);

		return Arrays.asList(
			new Object[]{genericCase},
			new Object[]{additionalClasses},
			new Object[]{differentOrder}
		);
	}

	public static final class DummyClassOne {

	}

	public static final class DummyClassTwo {

	}

}
