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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Animal;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Dog;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.DogKryoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.DogV2KryoSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoPojosForMigrationTests.Parrot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.testutils.ClassLoaderUtils;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

import static org.apache.flink.api.common.typeutils.TypeSerializerMatchers.isCompatibleAsIs;
import static org.apache.flink.api.common.typeutils.TypeSerializerMatchers.isCompatibleWithReconfiguredSerializer;
import static org.apache.flink.api.common.typeutils.TypeSerializerMatchers.isIncompatible;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for {@link KryoSerializerSnapshot}.
 */
public class KryoSerializerSnapshotTest {

	private ExecutionConfig oldConfig;
	private ExecutionConfig newConfig;

	@Before
	public void setup() {
		oldConfig = new ExecutionConfig();
		newConfig = new ExecutionConfig();
	}

	@Test
	public void sanityTest() {
		assertThat(resolveKryoCompatibility(oldConfig, newConfig), isCompatibleAsIs());
	}

	@Test
	public void addingTypesIsCompatibleAfterReconfiguration() {
		oldConfig.registerKryoType(Animal.class);

		newConfig.registerKryoType(Animal.class);
		newConfig.registerTypeWithKryoSerializer(Dog.class, DogKryoSerializer.class);

		assertThat(resolveKryoCompatibility(oldConfig, newConfig),
			isCompatibleWithReconfiguredSerializer());
	}

	@Test
	public void replacingKryoSerializersIsCompatibleAsIs() {
		oldConfig.registerKryoType(Animal.class);
		oldConfig.registerTypeWithKryoSerializer(Dog.class, DogKryoSerializer.class);

		newConfig.registerKryoType(Animal.class);
		newConfig.registerTypeWithKryoSerializer(Dog.class, DogV2KryoSerializer.class);

		// it is compatible as is, since Kryo does not expose compatibility API with KryoSerializers
		// so we can not know if DogKryoSerializer is compatible with DogV2KryoSerializer
		assertThat(resolveKryoCompatibility(oldConfig, newConfig),
			isCompatibleAsIs());
	}

	@Test
	public void reorderingIsCompatibleAfterReconfiguration() {
		oldConfig.registerKryoType(Parrot.class);
		oldConfig.registerKryoType(Dog.class);

		newConfig.registerKryoType(Dog.class);
		newConfig.registerKryoType(Parrot.class);

		assertThat(resolveKryoCompatibility(oldConfig, newConfig),
			isCompatibleWithReconfiguredSerializer());
	}

	@Test
	public void tryingToRestoreWithNonExistingClassShouldBeIncompatible() throws IOException {
		TypeSerializerSnapshot<Animal> restoredSnapshot = kryoSnapshotWithMissingClass();

		TypeSerializer<Animal> currentSerializer = new KryoSerializer<>(Animal.class, new ExecutionConfig());

		assertThat(restoredSnapshot.resolveSchemaCompatibility(currentSerializer),
			isIncompatible());
	}

	// -------------------------------------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------------------------------------

	private static TypeSerializerSnapshot<Animal> kryoSnapshotWithMissingClass() throws IOException {
		DataInputView in = new DataInputDeserializer(unLoadableSnapshotBytes());

		return TypeSerializerSnapshot.readVersionedSnapshot(
			in,
			KryoSerializerSnapshotTest.class.getClassLoader());
	}

	/**
	 * This method returns the bytes of a serialized {@link KryoSerializerSnapshot}, that contains a Kryo registration
	 * of a class that does not exists in the current classpath.
	 */
	private static byte[] unLoadableSnapshotBytes() throws IOException {
		final ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();

		final ClassLoaderUtils.ObjectAndClassLoader<Serializable> outsideClassLoading = ClassLoaderUtils.createSerializableObjectFromNewClassLoader();

		try {
			Thread.currentThread().setContextClassLoader(outsideClassLoading.getClassLoader());

			ExecutionConfig conf = new ExecutionConfig();
			conf.registerKryoType(outsideClassLoading.getObject().getClass());

			KryoSerializer<Animal> previousSerializer = new KryoSerializer<>(Animal.class, conf);
			TypeSerializerSnapshot<Animal> previousSnapshot = previousSerializer.snapshotConfiguration();

			DataOutputSerializer out = new DataOutputSerializer(4096);
			TypeSerializerSnapshot.writeVersionedSnapshot(out, previousSnapshot);
			return out.getCopyOfBuffer();
		}
		finally {
			Thread.currentThread().setContextClassLoader(originalClassLoader);
		}
	}

	private static TypeSerializerSchemaCompatibility<Animal> resolveKryoCompatibility(ExecutionConfig previous, ExecutionConfig current) {
		KryoSerializer<Animal> previousSerializer = new KryoSerializer<>(Animal.class, previous);
		TypeSerializerSnapshot<Animal> previousSnapshot = previousSerializer.snapshotConfiguration();

		TypeSerializer<Animal> currentSerializer = new KryoSerializer<>(Animal.class, current);
		return previousSnapshot.resolveSchemaCompatibility(currentSerializer);
	}
}
