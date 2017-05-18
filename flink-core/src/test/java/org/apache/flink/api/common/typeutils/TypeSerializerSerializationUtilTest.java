/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link TypeSerializerSerializationUtil}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TypeSerializerSerializationUtil.class)
public class TypeSerializerSerializationUtilTest {

	/**
	 * Verifies that reading and writing serializers work correctly.
	 */
	@Test
	public void testSerializerSerialization() throws Exception {

		TypeSerializer<?> serializer = IntSerializer.INSTANCE;

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			TypeSerializerSerializationUtil.writeSerializer(new DataOutputViewStreamWrapper(out), serializer);
			serialized = out.toByteArray();
		}

		TypeSerializer<?> deserializedSerializer;
		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			deserializedSerializer = TypeSerializerSerializationUtil.tryReadSerializer(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		Assert.assertEquals(serializer, deserializedSerializer);
	}

	/**
	 * Verifies deserialization failure cases when reading a serializer from bytes, in the
	 * case of a {@link ClassNotFoundException}.
	 */
	@Test
	public void testSerializerSerializationWithClassNotFound() throws Exception {

		TypeSerializer<?> serializer = IntSerializer.INSTANCE;

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			TypeSerializerSerializationUtil.writeSerializer(new DataOutputViewStreamWrapper(out), serializer);
			serialized = out.toByteArray();
		}

		TypeSerializer<?> deserializedSerializer;

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			deserializedSerializer = TypeSerializerSerializationUtil.tryReadSerializer(
				new DataInputViewStreamWrapper(in), new URLClassLoader(new URL[0], null));
		}
		Assert.assertEquals(null, deserializedSerializer);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			deserializedSerializer = TypeSerializerSerializationUtil.tryReadSerializer(
				new DataInputViewStreamWrapper(in), new URLClassLoader(new URL[0], null), true);
		}
		Assert.assertTrue(deserializedSerializer instanceof UnloadableDummyTypeSerializer);

		Assert.assertArrayEquals(
				InstantiationUtil.serializeObject(serializer),
				((UnloadableDummyTypeSerializer<?>) deserializedSerializer).getActualBytes());
	}

	/**
	 * Verifies deserialization failure cases when reading a serializer from bytes, in the
	 * case of a {@link InvalidClassException}.
	 */
	@Test
	public void testSerializerSerializationWithInvalidClass() throws Exception {

		TypeSerializer<?> serializer = IntSerializer.INSTANCE;

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			TypeSerializerSerializationUtil.writeSerializer(new DataOutputViewStreamWrapper(out), serializer);
			serialized = out.toByteArray();
		}

		TypeSerializer<?> deserializedSerializer;

		// mock failure when deserializing serializers
		TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<?> mockProxy =
				mock(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class);
		doThrow(new IOException()).when(mockProxy).read(any(DataInputViewStreamWrapper.class));
		PowerMockito.whenNew(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class).withAnyArguments().thenReturn(mockProxy);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			deserializedSerializer = TypeSerializerSerializationUtil.tryReadSerializer(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}
		Assert.assertEquals(null, deserializedSerializer);
	}

	/**
	 * Verifies that reading and writing configuration snapshots work correctly.
	 */
	@Test
	public void testSerializeConfigurationSnapshots() throws Exception {
		TypeSerializerSerializationUtilTest.TestConfigSnapshot configSnapshot1 =
			new TypeSerializerSerializationUtilTest.TestConfigSnapshot(1, "foo");

		TypeSerializerSerializationUtilTest.TestConfigSnapshot configSnapshot2 =
			new TypeSerializerSerializationUtilTest.TestConfigSnapshot(2, "bar");

		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSerializationUtil.writeSerializerConfigSnapshots(
				new DataOutputViewStreamWrapper(out),
				configSnapshot1,
				configSnapshot2);

			serializedConfig = out.toByteArray();
		}

		TypeSerializerConfigSnapshot[] restoredConfigs;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			restoredConfigs = TypeSerializerSerializationUtil.readSerializerConfigSnapshots(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		assertEquals(2, restoredConfigs.length);
		assertEquals(configSnapshot1, restoredConfigs[0]);
		assertEquals(configSnapshot2, restoredConfigs[1]);
	}

	/**
	 * Verifies that deserializing config snapshots fail if the config class could not be found.
	 */
	@Test
	public void testFailsWhenConfigurationSnapshotClassNotFound() throws Exception {
		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSerializationUtil.writeSerializerConfigSnapshot(
				new DataOutputViewStreamWrapper(out), new TypeSerializerSerializationUtilTest.TestConfigSnapshot(123, "foobar"));
			serializedConfig = out.toByteArray();
		}

		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			// read using a dummy classloader
			TypeSerializerSerializationUtil.readSerializerConfigSnapshot(
				new DataInputViewStreamWrapper(in), new URLClassLoader(new URL[0], null));
			fail("Expected a ClassNotFoundException wrapped in IOException");
		} catch (IOException expected) {
			// test passes
		}
	}

	/**
	 * Verifies resilience to serializer deserialization failures when writing and reading
	 * serializer and config snapshot pairs.
	 */
	@Test
	public void testSerializerAndConfigPairsSerializationWithSerializerDeserializationFailures() throws Exception {
		List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersAndConfigs = Arrays.asList(
			new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(
				IntSerializer.INSTANCE, IntSerializer.INSTANCE.snapshotConfiguration()),
			new Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>(
				DoubleSerializer.INSTANCE, DoubleSerializer.INSTANCE.snapshotConfiguration()));

		byte[] serializedSerializersAndConfigs;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
					new DataOutputViewStreamWrapper(out), serializersAndConfigs);
			serializedSerializersAndConfigs = out.toByteArray();
		}

		// mock failure when deserializing serializers
		TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<?> mockProxy =
				mock(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class);
		doThrow(new IOException()).when(mockProxy).read(any(DataInputViewStreamWrapper.class));
		PowerMockito.whenNew(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class).withAnyArguments().thenReturn(mockProxy);

		List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> restored;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedSerializersAndConfigs)) {
			restored = TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		Assert.assertEquals(2, restored.size());
		Assert.assertEquals(null, restored.get(0).f0);
		Assert.assertEquals(IntSerializer.INSTANCE.snapshotConfiguration(), restored.get(0).f1);
		Assert.assertEquals(null, restored.get(1).f0);
		Assert.assertEquals(DoubleSerializer.INSTANCE.snapshotConfiguration(), restored.get(1).f1);
	}

	public static class TestConfigSnapshot extends TypeSerializerConfigSnapshot {

		static final int VERSION = 1;

		private int val;
		private String msg;

		public TestConfigSnapshot() {}

		public TestConfigSnapshot(int val, String msg) {
			this.val = val;
			this.msg = msg;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			super.write(out);
			out.writeInt(val);
			out.writeUTF(msg);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			super.read(in);
			val = in.readInt();
			msg = in.readUTF();
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			}

			if (obj == null) {
				return false;
			}

			if (obj instanceof TypeSerializerSerializationUtilTest.TestConfigSnapshot) {
				return val == ((TypeSerializerSerializationUtilTest.TestConfigSnapshot) obj).val
					&& msg.equals(((TypeSerializerSerializationUtilTest.TestConfigSnapshot) obj).msg);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return 31 * val + msg.hashCode();
		}
	}
}
