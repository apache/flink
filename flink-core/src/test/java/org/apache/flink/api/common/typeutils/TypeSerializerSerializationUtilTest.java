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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.testutils.ArtificialCNFExceptionThrowingClassLoader;
import org.apache.flink.util.InstantiationUtil;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link TypeSerializerSerializationUtil}.
 */
public class TypeSerializerSerializationUtilTest implements Serializable {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

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
				new DataInputViewStreamWrapper(in),
				new ArtificialCNFExceptionThrowingClassLoader(
					Thread.currentThread().getContextClassLoader(),
					Collections.singleton(IntSerializer.class.getName())),
				true);
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

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			deserializedSerializer = TypeSerializerSerializationUtil.tryReadSerializer(
				new DataInputViewStreamWrapper(in),
				new ArtificialCNFExceptionThrowingClassLoader(
					Thread.currentThread().getContextClassLoader(),
					Collections.singleton(IntSerializer.class.getName())),
				true);
		}
		Assert.assertTrue(deserializedSerializer instanceof UnloadableDummyTypeSerializer);
	}

	/**
	 * Verifies that reading and writing configuration snapshots work correctly.
	 */
	@Test
	public void testSerializeConfigurationSnapshots() throws Exception {
		TypeSerializerSerializationUtilTest.TestConfigSnapshot<String> configSnapshot1 =
			new TypeSerializerSerializationUtilTest.TestConfigSnapshot<>(1, "foo");

		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
				new DataOutputViewStreamWrapper(out),
				configSnapshot1,
				StringSerializer.INSTANCE);

			serializedConfig = out.toByteArray();
		}

		TypeSerializerSnapshot<?> restoredConfigs;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			restoredConfigs = TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader(), null);
		}

		assertEquals(configSnapshot1, restoredConfigs);
	}

	/**
	 * Verifies that deserializing config snapshots fail if the config class could not be found.
	 */
	@Test(expected = IOException.class)
	public void testFailsWhenConfigurationSnapshotClassNotFound() throws Exception {
		byte[] serializedConfig;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
				new DataOutputViewStreamWrapper(out),
				new TypeSerializerSerializationUtilTest.TestConfigSnapshot<>(123, "foobar"),
				StringSerializer.INSTANCE);
			serializedConfig = out.toByteArray();
		}

		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
			// read using a dummy classloader
			TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
				new DataInputViewStreamWrapper(in), new URLClassLoader(new URL[0], null), null);
		}

		fail("Expected a ClassNotFoundException wrapped in IOException");
	}

	/**
	 * Verifies resilience to serializer deserialization failures when writing and reading
	 * serializer and config snapshot pairs.
	 */
	@Test
	public void testSerializerAndConfigPairsSerializationWithSerializerDeserializationFailures() throws Exception {
		TestIntSerializer serializer = new TestIntSerializer();

		List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> serializersAndConfigs = Arrays.asList(
			new Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>(
				serializer, serializer.snapshotConfiguration()));

		byte[] serializedSerializersAndConfigs;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSerializationUtil.writeSerializersAndConfigsWithResilience(
					new DataOutputViewStreamWrapper(out), serializersAndConfigs);
			serializedSerializersAndConfigs = out.toByteArray();
		}

		Set<String> cnfThrowingClassnames = new HashSet<>();
		cnfThrowingClassnames.add(TestIntSerializer.class.getName());

		List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> restored;
		try (ByteArrayInputStream in = new ByteArrayInputStream(serializedSerializersAndConfigs)) {
			restored = TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(
				new DataInputViewStreamWrapper(in),
				new ArtificialCNFExceptionThrowingClassLoader(
					Thread.currentThread().getContextClassLoader(),
					cnfThrowingClassnames));
		}

		Assert.assertEquals(1, restored.size());
		Assert.assertTrue(restored.get(0).f0 instanceof UnloadableDummyTypeSerializer);
		Assert.assertThat(restored.get(0).f1, Matchers.instanceOf(SimpleTypeSerializerSnapshot.class));
	}

	/**
	 * Verifies that serializers of anonymous classes can be deserialized, even if serialVersionUID changes.
	 */
	@Test
	public void testAnonymousSerializerClassWithChangedSerialVersionUID() throws Exception {

		TypeSerializer anonymousClassSerializer = new AbstractIntSerializer() {};
		// assert that our assumption holds
		Assert.assertTrue(anonymousClassSerializer.getClass().isAnonymousClass());

		byte[] anonymousSerializerBytes;
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			TypeSerializerSerializationUtil.writeSerializer(new DataOutputViewStreamWrapper(out), anonymousClassSerializer);
			anonymousSerializerBytes = out.toByteArray();
		}

		long newSerialVersionUID = 1234567L;
		// assert that we're actually modifying to a different serialVersionUID
		Assert.assertNotEquals(ObjectStreamClass.lookup(anonymousClassSerializer.getClass()).getSerialVersionUID(), newSerialVersionUID);
		modifySerialVersionUID(anonymousSerializerBytes, anonymousClassSerializer.getClass().getName(), newSerialVersionUID);

		try (ByteArrayInputStream in = new ByteArrayInputStream(anonymousSerializerBytes)) {
			anonymousClassSerializer = TypeSerializerSerializationUtil.tryReadSerializer(new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		// serializer should have been deserialized despite serialVersionUID mismatch
		Assert.assertNotNull(anonymousClassSerializer);
		Assert.assertTrue(anonymousClassSerializer.getClass().isAnonymousClass());
	}

	public static class TestConfigSnapshot<T> extends TypeSerializerConfigSnapshot<T> {

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

	private static void modifySerialVersionUID(byte[] objectBytes, String classname, long newSerialVersionUID) throws Exception {
		byte[] classnameBytes = classname.getBytes();

		// serialVersionUID follows directly after classname in the object byte stream;
		// advance serialVersionUIDPosition until end of classname in stream
		int serialVersionUIDOffset;
		boolean foundClass = false;
		int numMatchedBytes = 0;
		for (serialVersionUIDOffset = 0; serialVersionUIDOffset < objectBytes.length; serialVersionUIDOffset++) {
			if (objectBytes[serialVersionUIDOffset] == classnameBytes[numMatchedBytes]) {
				numMatchedBytes++;
				foundClass = true;
			} else {
				if (objectBytes[serialVersionUIDOffset] == classnameBytes[0]) {
					numMatchedBytes = 1;
				} else {
					numMatchedBytes = 0;
					foundClass = false;
				}
			}

			if (numMatchedBytes == classnameBytes.length) {
				break;
			}
		}

		if (!foundClass) {
			throw new RuntimeException("Could not find class " + classname + " in object byte stream.");
		}

		byte[] newUIDBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(newSerialVersionUID).array();

		// replace original serialVersionUID bytes with new serialVersionUID bytes
		for (int uidIndex = 0; uidIndex < newUIDBytes.length; uidIndex++) {
			objectBytes[serialVersionUIDOffset + 1 + uidIndex] = newUIDBytes[uidIndex];
		}
	}

	public static abstract class AbstractIntSerializer extends TypeSerializer<Integer> {

		private static final long serialVersionUID = 1;

		@Override
		public Integer createInstance() {
			return IntSerializer.INSTANCE.createInstance();
		}

		@Override
		public boolean isImmutableType() {
			return IntSerializer.INSTANCE.isImmutableType();
		}

		@Override
		public Integer copy(Integer from) {
			return IntSerializer.INSTANCE.copy(from);
		}

		@Override
		public Integer copy(Integer from, Integer reuse) {
			return IntSerializer.INSTANCE.copy(from, reuse);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			IntSerializer.INSTANCE.copy(source, target);
		}

		@Override
		public Integer deserialize(DataInputView source) throws IOException {
			return IntSerializer.INSTANCE.deserialize(source);
		}

		@Override
		public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
			return IntSerializer.INSTANCE.deserialize(reuse, source);
		}

		@Override
		public void serialize(Integer record, DataOutputView target) throws IOException {
			IntSerializer.INSTANCE.serialize(record, target);
		}

		@Override
		public TypeSerializer<Integer> duplicate() {
			return IntSerializer.INSTANCE.duplicate();
		}

		@Override
		public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
			return IntSerializer.INSTANCE.snapshotConfiguration();
		}

		@Override
		public CompatibilityResult<Integer> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
			return IntSerializer.INSTANCE.ensureCompatibility(configSnapshot);
		}

		@Override
		public int getLength() {
			return IntSerializer.INSTANCE.getLength();
		}

		@Override
		public boolean canEqual(Object obj) {
			return IntSerializer.INSTANCE.canEqual(obj);
		}

		@Override
		public boolean equals(Object obj) {
			return IntSerializer.INSTANCE.equals(obj);
		}

		@Override
		public int hashCode() {
			return IntSerializer.INSTANCE.hashCode();
		}
	}

	/** Just some serializer used for tests. */
	public static class TestIntSerializer extends AbstractIntSerializer {
		private static final long serialVersionUID = -3684467698271707216L;
	}
}
