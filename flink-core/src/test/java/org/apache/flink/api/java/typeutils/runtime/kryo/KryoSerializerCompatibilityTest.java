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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to configuration snapshotting and reconfiguring for the {@link KryoSerializer}.
 */
public class KryoSerializerCompatibilityTest {

	/**
	 * Verifies that reconfiguration result is INCOMPATIBLE if data type has changed.
	 */
	@Test
	public void testMigrationStrategyWithDifferentKryoType() throws Exception {
		KryoSerializer<TestClassA> kryoSerializerForA = new KryoSerializer<>(TestClassA.class, new ExecutionConfig());

		// snapshot configuration and serialize to bytes
		TypeSerializerConfigSnapshot kryoSerializerConfigSnapshot = kryoSerializerForA.snapshotConfiguration();
		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSerializationUtil.writeSerializerConfigSnapshot(new DataOutputViewStreamWrapper(out), kryoSerializerConfigSnapshot);
			serializedConfig = out.toByteArray();
		}

		KryoSerializer<TestClassB> kryoSerializerForB = new KryoSerializer<>(TestClassB.class, new ExecutionConfig());

		// read configuration again from bytes
		try(ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			kryoSerializerConfigSnapshot = TypeSerializerSerializationUtil.readSerializerConfigSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		CompatibilityResult<TestClassB> compatResult = kryoSerializerForB.ensureCompatibility(kryoSerializerConfigSnapshot);
		assertTrue(compatResult.isRequiresMigration());
	}

	/**
	 * Tests that after reconfiguration, registration ids are reconfigured to
	 * remain the same as the preceding KryoSerializer.
	 */
	@Test
	public void testMigrationStrategyForDifferentRegistrationOrder() throws Exception {

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.registerKryoType(TestClassA.class);
		executionConfig.registerKryoType(TestClassB.class);

		KryoSerializer<TestClass> kryoSerializer = new KryoSerializer<>(TestClass.class, executionConfig);

		// get original registration ids
		int testClassId = kryoSerializer.getKryo().getRegistration(TestClass.class).getId();
		int testClassAId = kryoSerializer.getKryo().getRegistration(TestClassA.class).getId();
		int testClassBId = kryoSerializer.getKryo().getRegistration(TestClassB.class).getId();

		// snapshot configuration and serialize to bytes
		TypeSerializerConfigSnapshot kryoSerializerConfigSnapshot = kryoSerializer.snapshotConfiguration();
		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSerializationUtil.writeSerializerConfigSnapshot(new DataOutputViewStreamWrapper(out), kryoSerializerConfigSnapshot);
			serializedConfig = out.toByteArray();
		}

		// use new config and instantiate new KryoSerializer
		executionConfig = new ExecutionConfig();
		executionConfig.registerKryoType(TestClassB.class); // test with B registered before A
		executionConfig.registerKryoType(TestClassA.class);

		kryoSerializer = new KryoSerializer<>(TestClass.class, executionConfig);

		// read configuration from bytes
		try(ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			kryoSerializerConfigSnapshot = TypeSerializerSerializationUtil.readSerializerConfigSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		// reconfigure - check reconfiguration result and that registration id remains the same
		CompatibilityResult<TestClass> compatResult = kryoSerializer.ensureCompatibility(kryoSerializerConfigSnapshot);
		assertFalse(compatResult.isRequiresMigration());
		assertEquals(testClassId, kryoSerializer.getKryo().getRegistration(TestClass.class).getId());
		assertEquals(testClassAId, kryoSerializer.getKryo().getRegistration(TestClassA.class).getId());
		assertEquals(testClassBId, kryoSerializer.getKryo().getRegistration(TestClassB.class).getId());
	}

	private static class TestClass {}

	private static class TestClassA {}

	private static class TestClassB {}

	private static class TestClassBSerializer extends Serializer {
		@Override
		public void write(Kryo kryo, Output output, Object o) {
			throw new UnsupportedOperationException();
		}

		@Override
		public Object read(Kryo kryo, Input input, Class aClass) {
			throw new UnsupportedOperationException();
		}
	}
}
