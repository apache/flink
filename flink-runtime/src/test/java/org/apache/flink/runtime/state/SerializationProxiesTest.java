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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoReader;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;
import org.apache.flink.testutils.ArtificialCNFExceptionThrowingClassLoader;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.CURRENT_STATE_META_INFO_SNAPSHOT_VERSION;

public class SerializationProxiesTest {

	@Test
	public void testKeyedBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<StateMetaInfoSnapshot> stateMetaInfoList = new ArrayList<>();

		stateMetaInfoList.add(new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, "a", namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, "b", namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, "c", namespaceSerializer, stateSerializer).snapshot());

		KeyedBackendSerializationProxy<?> serializationProxy =
				new KeyedBackendSerializationProxy<>(keySerializer, stateMetaInfoList, true);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			serializationProxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		serializationProxy =
				new KeyedBackendSerializationProxy<>(Thread.currentThread().getContextClassLoader(), true);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertTrue(serializationProxy.isUsingKeyGroupCompression());
		Assert.assertEquals(keySerializer, serializationProxy.getKeySerializer());
		Assert.assertEquals(keySerializer.snapshotConfiguration(), serializationProxy.getKeySerializerConfigSnapshot());
		assertEqualStateMetaInfoSnapshotsLists(stateMetaInfoList, serializationProxy.getStateMetaInfoSnapshots());
	}

	@Test
	public void testKeyedBackendSerializationProxyRoundtripWithSerializerSerializationFailures() throws Exception {

		TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<StateMetaInfoSnapshot> stateMetaInfoList = new ArrayList<>();

		stateMetaInfoList.add(new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, "a", namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, "b", namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, "c", namespaceSerializer, stateSerializer).snapshot());

		KeyedBackendSerializationProxy<?> serializationProxy =
			new KeyedBackendSerializationProxy<>(keySerializer, stateMetaInfoList, true);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			serializationProxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		Set<String> cnfThrowingSerializerClasses = new HashSet<>();
		cnfThrowingSerializerClasses.add(IntSerializer.class.getName());
		cnfThrowingSerializerClasses.add(LongSerializer.class.getName());
		cnfThrowingSerializerClasses.add(DoubleSerializer.class.getName());

		// we want to verify restore resilience when serializer presence is not required;
		// set isSerializerPresenceRequired to false
		serializationProxy =
			new KeyedBackendSerializationProxy<>(
				new ArtificialCNFExceptionThrowingClassLoader(
					Thread.currentThread().getContextClassLoader(),
					cnfThrowingSerializerClasses),
				false);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(true, serializationProxy.isUsingKeyGroupCompression());
		Assert.assertTrue(serializationProxy.getKeySerializer() instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(keySerializer.snapshotConfiguration(), serializationProxy.getKeySerializerConfigSnapshot());

		for (StateMetaInfoSnapshot snapshot : serializationProxy.getStateMetaInfoSnapshots()) {
			final RegisteredKeyedBackendStateMetaInfo<?, ?> restoredMetaInfo = new RegisteredKeyedBackendStateMetaInfo<>(snapshot);
			Assert.assertTrue(restoredMetaInfo.getNamespaceSerializer() instanceof UnloadableDummyTypeSerializer);
			Assert.assertTrue(restoredMetaInfo.getStateSerializer() instanceof UnloadableDummyTypeSerializer);
			Assert.assertEquals(namespaceSerializer.snapshotConfiguration(), snapshot.getTypeSerializerConfigSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER));
			Assert.assertEquals(stateSerializer.snapshotConfiguration(), snapshot.getTypeSerializerConfigSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER));
		}
	}

	@Test
	public void testKeyedStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		StateMetaInfoSnapshot metaInfo = new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, name, namespaceSerializer, stateSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(metaInfo, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.KEYED_STATE);
			metaInfo = reader.readStateMetaInfoSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		Assert.assertEquals(name, metaInfo.getName());
	}

	@Test
	public void testKeyedStateMetaInfoReadSerializerFailureResilience() throws Exception {
		String name = "test";
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		StateMetaInfoSnapshot snapshot = new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, name, namespaceSerializer, stateSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(snapshot, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		Set<String> cnfThrowingSerializerClasses = new HashSet<>();
		cnfThrowingSerializerClasses.add(LongSerializer.class.getName());
		cnfThrowingSerializerClasses.add(DoubleSerializer.class.getName());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.KEYED_STATE);
			final ClassLoader classLoader = new ArtificialCNFExceptionThrowingClassLoader(
				Thread.currentThread().getContextClassLoader(),
				cnfThrowingSerializerClasses);

			snapshot = reader.readStateMetaInfoSnapshot(
				new DataInputViewStreamWrapper(in), classLoader);
		}

		RegisteredKeyedBackendStateMetaInfo<?, ?> restoredMetaInfo = new RegisteredKeyedBackendStateMetaInfo<>(snapshot);

		Assert.assertEquals(name, restoredMetaInfo.getName());
		Assert.assertTrue(restoredMetaInfo.getNamespaceSerializer() instanceof UnloadableDummyTypeSerializer);
		Assert.assertTrue(restoredMetaInfo.getStateSerializer() instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(namespaceSerializer.snapshotConfiguration(), snapshot.getTypeSerializerConfigSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER));
		Assert.assertEquals(stateSerializer.snapshotConfiguration(), snapshot.getTypeSerializerConfigSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	@Test
	public void testOperatorBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> keySerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> valueSerializer = StringSerializer.INSTANCE;

		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>();

		stateMetaInfoSnapshots.add(new RegisteredOperatorBackendStateMetaInfo<>(
			"a", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE).snapshot());
		stateMetaInfoSnapshots.add(new RegisteredOperatorBackendStateMetaInfo<>(
			"b", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE).snapshot());
		stateMetaInfoSnapshots.add(new RegisteredOperatorBackendStateMetaInfo<>(
			"c", stateSerializer, OperatorStateHandle.Mode.UNION).snapshot());

		List<StateMetaInfoSnapshot> broadcastStateMetaInfoSnapshots = new ArrayList<>();

		broadcastStateMetaInfoSnapshots.add(new RegisteredBroadcastBackendStateMetaInfo<>(
				"d", OperatorStateHandle.Mode.BROADCAST, keySerializer, valueSerializer).snapshot());
		broadcastStateMetaInfoSnapshots.add(new RegisteredBroadcastBackendStateMetaInfo<>(
				"e", OperatorStateHandle.Mode.BROADCAST, valueSerializer, keySerializer).snapshot());

		OperatorBackendSerializationProxy serializationProxy =
				new OperatorBackendSerializationProxy(stateMetaInfoSnapshots, broadcastStateMetaInfoSnapshots);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			serializationProxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		serializationProxy =
				new OperatorBackendSerializationProxy(Thread.currentThread().getContextClassLoader());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		assertEqualStateMetaInfoSnapshotsLists(stateMetaInfoSnapshots, serializationProxy.getOperatorStateMetaInfoSnapshots());
		assertEqualStateMetaInfoSnapshotsLists(broadcastStateMetaInfoSnapshots, serializationProxy.getBroadcastStateMetaInfoSnapshots());
	}

	@Test
	public void testOperatorStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		StateMetaInfoSnapshot snapshot =
			new RegisteredOperatorBackendStateMetaInfo<>(
				name, stateSerializer, OperatorStateHandle.Mode.UNION).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().writeStateMetaInfoSnapshot(snapshot, new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);
			snapshot = reader.readStateMetaInfoSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		RegisteredOperatorBackendStateMetaInfo<?> restoredMetaInfo =
			new RegisteredOperatorBackendStateMetaInfo<>(snapshot);

		Assert.assertEquals(name, restoredMetaInfo.getName());
		Assert.assertEquals(OperatorStateHandle.Mode.UNION, restoredMetaInfo.getAssignmentMode());
		Assert.assertEquals(stateSerializer, restoredMetaInfo.getPartitionStateSerializer());
	}

	@Test
	public void testBroadcastStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> keySerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> valueSerializer = StringSerializer.INSTANCE;

		StateMetaInfoSnapshot snapshot =
			new RegisteredBroadcastBackendStateMetaInfo<>(
				name, OperatorStateHandle.Mode.BROADCAST, keySerializer, valueSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(snapshot, new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);
			snapshot = reader.readStateMetaInfoSnapshot(
				new DataInputViewStreamWrapper(in), Thread.currentThread().getContextClassLoader());
		}

		RegisteredBroadcastBackendStateMetaInfo<?, ?> restoredMetaInfo =
			new RegisteredBroadcastBackendStateMetaInfo<>(snapshot);

		Assert.assertEquals(name, restoredMetaInfo.getName());
		Assert.assertEquals(
			OperatorStateHandle.Mode.BROADCAST,
			restoredMetaInfo.getAssignmentMode());
		Assert.assertEquals(keySerializer, restoredMetaInfo.getKeySerializer());
		Assert.assertEquals(valueSerializer, restoredMetaInfo.getValueSerializer());
	}

	@Test
	public void testOperatorStateMetaInfoReadSerializerFailureResilience() throws Exception {
		String name = "test";
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		StateMetaInfoSnapshot snapshot =
			new RegisteredOperatorBackendStateMetaInfo<>(
				name, stateSerializer, OperatorStateHandle.Mode.UNION).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(snapshot, new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		Set<String> cnfThrowingSerializerClasses = new HashSet<>();
		cnfThrowingSerializerClasses.add(DoubleSerializer.class.getName());
		cnfThrowingSerializerClasses.add(StringSerializer.class.getName());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(
				CURRENT_STATE_META_INFO_SNAPSHOT_VERSION, StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);
			final ClassLoader classLoader = new ArtificialCNFExceptionThrowingClassLoader(
				Thread.currentThread().getContextClassLoader(),
				cnfThrowingSerializerClasses);
			snapshot = reader.readStateMetaInfoSnapshot(new DataInputViewStreamWrapper(in), classLoader);
		}

		RegisteredOperatorBackendStateMetaInfo<?> restoredMetaInfo =
			new RegisteredOperatorBackendStateMetaInfo<>(snapshot);

		Assert.assertEquals(name, restoredMetaInfo.getName());
		Assert.assertTrue(restoredMetaInfo.getPartitionStateSerializer() instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(
			stateSerializer.snapshotConfiguration(),
			snapshot.getTypeSerializerConfigSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	@Test
	public void testBroadcastStateMetaInfoReadSerializerFailureResilience() throws Exception {
		String broadcastName = "broadcastTest";
		TypeSerializer<?> keySerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> valueSerializer = StringSerializer.INSTANCE;

		StateMetaInfoSnapshot snapshot =
			new RegisteredBroadcastBackendStateMetaInfo<>(
				broadcastName, OperatorStateHandle.Mode.BROADCAST, keySerializer, valueSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			StateMetaInfoSnapshotReadersWriters.getWriter().
				writeStateMetaInfoSnapshot(snapshot, new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		Set<String> cnfThrowingSerializerClasses = new HashSet<>();
		cnfThrowingSerializerClasses.add(DoubleSerializer.class.getName());
		cnfThrowingSerializerClasses.add(StringSerializer.class.getName());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			final StateMetaInfoReader reader =
				StateMetaInfoSnapshotReadersWriters.getReader(
					CURRENT_STATE_META_INFO_SNAPSHOT_VERSION,
					StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);

			final ClassLoader classLoader = new ArtificialCNFExceptionThrowingClassLoader(
				Thread.currentThread().getContextClassLoader(),
				cnfThrowingSerializerClasses);

			snapshot = reader.readStateMetaInfoSnapshot(new DataInputViewStreamWrapper(in), classLoader);
		}

		RegisteredBroadcastBackendStateMetaInfo<?, ?> restoredMetaInfo =
			new RegisteredBroadcastBackendStateMetaInfo<>(snapshot);

		Assert.assertEquals(broadcastName, restoredMetaInfo.getName());
		Assert.assertEquals(OperatorStateHandle.Mode.BROADCAST, restoredMetaInfo.getAssignmentMode());
		Assert.assertTrue(restoredMetaInfo.getKeySerializer() instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(keySerializer.snapshotConfiguration(), snapshot.getTypeSerializerConfigSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.KEY_SERIALIZER));
		Assert.assertTrue(restoredMetaInfo.getValueSerializer() instanceof UnloadableDummyTypeSerializer);
		Assert.assertEquals(valueSerializer.snapshotConfiguration(), snapshot.getTypeSerializerConfigSnapshot(StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER));
	}

	/**
	 * This test fixes the order of elements in the enum which is important for serialization. Do not modify this test
	 * except if you are entirely sure what you are doing.
	 */
	@Test
	public void testFixTypeOrder() {
		// ensure all elements are covered
		Assert.assertEquals(7, StateDescriptor.Type.values().length);
		// fix the order of elements to keep serialization format stable
		Assert.assertEquals(0, StateDescriptor.Type.UNKNOWN.ordinal());
		Assert.assertEquals(1, StateDescriptor.Type.VALUE.ordinal());
		Assert.assertEquals(2, StateDescriptor.Type.LIST.ordinal());
		Assert.assertEquals(3, StateDescriptor.Type.REDUCING.ordinal());
		Assert.assertEquals(4, StateDescriptor.Type.FOLDING.ordinal());
		Assert.assertEquals(5, StateDescriptor.Type.AGGREGATING.ordinal());
		Assert.assertEquals(6, StateDescriptor.Type.MAP.ordinal());
	}

	private void assertEqualStateMetaInfoSnapshotsLists(
		List<StateMetaInfoSnapshot> expected,
		List<StateMetaInfoSnapshot> actual) {
		Assert.assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); ++i) {
			assertEqualStateMetaInfoSnapshots(expected.get(i), actual.get(i));
		}
	}

	private void assertEqualStateMetaInfoSnapshots(StateMetaInfoSnapshot expected, StateMetaInfoSnapshot actual) {
		Assert.assertEquals(expected.getName(), actual.getName());
		Assert.assertEquals(expected.getBackendStateType(), actual.getBackendStateType());
		Assert.assertEquals(expected.getOptionsImmutable(), actual.getOptionsImmutable());
		Assert.assertEquals(expected.getSerializersImmutable(), actual.getSerializersImmutable());
		Assert.assertEquals(
			expected.getSerializerConfigSnapshotsImmutable(),
			actual.getSerializerConfigSnapshotsImmutable());
	}
}
