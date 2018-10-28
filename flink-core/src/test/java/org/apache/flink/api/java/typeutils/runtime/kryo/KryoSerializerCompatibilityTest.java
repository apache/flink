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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests related to configuration snapshotting and reconfiguring for the {@link KryoSerializer}.
 */
public class KryoSerializerCompatibilityTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testMigrationStrategyForRemovedAvroDependency() throws Exception {
		KryoSerializer<TestClass> kryoSerializerForA = new KryoSerializer<>(TestClass.class, new ExecutionConfig());

		// read configuration again from bytes
		TypeSerializerSnapshot kryoSerializerConfigSnapshot;
		try (InputStream in = getClass().getResourceAsStream("/kryo-serializer-flink1.3-snapshot")) {
			kryoSerializerConfigSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader(), kryoSerializerForA);
		}

		@SuppressWarnings("unchecked")
		TypeSerializerSchemaCompatibility<TestClass> compatResult =
			kryoSerializerConfigSnapshot.resolveSchemaCompatibility(kryoSerializerForA);
		assertTrue(compatResult.isCompatibleAsIs());
	}

	@Test
	public void testDeserializingKryoSerializerWithoutAvro() throws Exception {
		final String resource = "serialized-kryo-serializer-1.3";

		TypeSerializer<?> serializer;

		try (InputStream in = getClass().getClassLoader().getResourceAsStream(resource)) {
			DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(in);

			serializer = TypeSerializerSerializationUtil.tryReadSerializer(inView, getClass().getClassLoader());
		}

		assertNotNull(serializer);
		assertTrue(serializer instanceof KryoSerializer);
	}

	/**
	 * Verifies that reconfiguration result is INCOMPATIBLE if data type has changed.
	 */
	@Test
	public void testMigrationStrategyWithDifferentKryoType() throws Exception {
		KryoSerializer<TestClassA> kryoSerializerForA = new KryoSerializer<>(TestClassA.class, new ExecutionConfig());

		// snapshot configuration and serialize to bytes
		TypeSerializerSnapshot kryoSerializerConfigSnapshot = kryoSerializerForA.snapshotConfiguration();
		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
				new DataOutputViewStreamWrapper(out), kryoSerializerConfigSnapshot, kryoSerializerForA);
			serializedConfig = out.toByteArray();
		}

		KryoSerializer<TestClassB> kryoSerializerForB = new KryoSerializer<>(TestClassB.class, new ExecutionConfig());

		// read configuration again from bytes
		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			kryoSerializerConfigSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader(), kryoSerializerForB);
		}

		@SuppressWarnings("unchecked")
		TypeSerializerSchemaCompatibility<TestClassB> compatResult =
			kryoSerializerConfigSnapshot.resolveSchemaCompatibility(kryoSerializerForB);
		assertTrue(compatResult.isIncompatible());
	}

	@Test
	public void testMigrationOfTypeWithAvroType() throws Exception {

		/*
		 When Avro sees the schema "{"type" : "array", "items" : "boolean"}" it will create a field
		 of type List<Integer> but the actual type will be GenericData.Array<Integer>. The
		 KryoSerializer registers a special Serializer for this type that simply deserializes
		 as ArrayList because Kryo cannot handle GenericData.Array well. Before Flink 1.4 Avro
		 was always in the classpath but after 1.4 it's only present if the flink-avro jar is
		 included. This test verifies that we can still deserialize data written pre-1.4.
		 */
		class FakeAvroClass {
			public List<Integer> array;

			FakeAvroClass(List<Integer> array) {
				this.array = array;
			}
		}

		/*
		// This has to be executed on a pre-1.4 branch to generate the binary blob
		{
			ExecutionConfig executionConfig = new ExecutionConfig();
			KryoSerializer<FakeAvroClass> kryoSerializer =
				new KryoSerializer<>(FakeAvroClass.class, executionConfig);

			try (
				FileOutputStream f = new FileOutputStream(
					"src/test/resources/type-with-avro-serialized-using-kryo");
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(f)) {


				GenericData.Array<Integer> array =
					new GenericData.Array<>(10, Schema.createArray(Schema.create(Schema.Type.INT)));

				array.add(10);
				array.add(20);
				array.add(30);

				FakeAvroClass myTestClass = new FakeAvroClass(array);

				kryoSerializer.serialize(myTestClass, outputView);
			}
		}
		*/

		{
			ExecutionConfig executionConfig = new ExecutionConfig();
			KryoSerializer<FakeAvroClass> kryoSerializer =
				new KryoSerializer<>(FakeAvroClass.class, executionConfig);

			try (
				FileInputStream f = new FileInputStream("src/test/resources/type-with-avro-serialized-using-kryo");
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(f)) {

				thrown.expectMessage("Could not find required Avro dependency");
				kryoSerializer.deserialize(inputView);
			}
		}
	}

	@Test
	public void testMigrationWithTypeDevoidOfAvroTypes() throws Exception {

		class FakeClass {
			public List<Integer> array;

			FakeClass(List<Integer> array) {
				this.array = array;
			}
		}

		/*
		// This has to be executed on a pre-1.4 branch to generate the binary blob
		{
			ExecutionConfig executionConfig = new ExecutionConfig();
			KryoSerializer<FakeClass> kryoSerializer =
				new KryoSerializer<>(FakeClass.class, executionConfig);

			try (
				FileOutputStream f = new FileOutputStream(
					"src/test/resources/type-without-avro-serialized-using-kryo");
				DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(f)) {


				List<Integer> array = new ArrayList<>(10);

				array.add(10);
				array.add(20);
				array.add(30);

				FakeClass myTestClass = new FakeClass(array);

				kryoSerializer.serialize(myTestClass, outputView);
			}
		}
		*/

		{
			ExecutionConfig executionConfig = new ExecutionConfig();
			KryoSerializer<FakeClass> kryoSerializer =
				new KryoSerializer<>(FakeClass.class, executionConfig);

			try (
				FileInputStream f = new FileInputStream("src/test/resources/type-without-avro-serialized-using-kryo");
				DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(f)) {

				FakeClass myTestClass = kryoSerializer.deserialize(inputView);

				assertThat(myTestClass.array.get(0), is(10));
				assertThat(myTestClass.array.get(1), is(20));
				assertThat(myTestClass.array.get(2), is(30));
			}
		}
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
		TypeSerializerSnapshot kryoSerializerConfigSnapshot = kryoSerializer.snapshotConfiguration();
		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
				new DataOutputViewStreamWrapper(out), kryoSerializerConfigSnapshot, kryoSerializer);
			serializedConfig = out.toByteArray();
		}

		// use new config and instantiate new KryoSerializer
		executionConfig = new ExecutionConfig();
		executionConfig.registerKryoType(TestClassB.class); // test with B registered before A
		executionConfig.registerKryoType(TestClassA.class);

		kryoSerializer = new KryoSerializer<>(TestClass.class, executionConfig);

		// read configuration from bytes
		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			kryoSerializerConfigSnapshot = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader(), kryoSerializer);
		}

		// reconfigure - check reconfiguration result and that registration id remains the same
		@SuppressWarnings("unchecked")
		TypeSerializerSchemaCompatibility<TestClass> compatResult =
			kryoSerializerConfigSnapshot.resolveSchemaCompatibility(kryoSerializer);
		assertTrue(compatResult.isCompatibleAsIs());
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
