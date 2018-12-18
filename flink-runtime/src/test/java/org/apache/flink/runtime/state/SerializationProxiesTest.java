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

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.CURRENT_STATE_META_INFO_SNAPSHOT_VERSION;

public class SerializationProxiesTest {

	@Test
	public void testKeyedBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<StateMetaInfoSnapshot> stateMetaInfoList = new ArrayList<>();

		stateMetaInfoList.add(new RegisteredKeyValueStateBackendMetaInfo<>(
			StateDescriptor.Type.VALUE, "a", namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(new RegisteredKeyValueStateBackendMetaInfo<>(
			StateDescriptor.Type.VALUE, "b", namespaceSerializer, stateSerializer).snapshot());
		stateMetaInfoList.add(new RegisteredKeyValueStateBackendMetaInfo<>(
			StateDescriptor.Type.VALUE, "c", namespaceSerializer, stateSerializer).snapshot());

		KeyedBackendSerializationProxy<?> serializationProxy =
				new KeyedBackendSerializationProxy<>(keySerializer, stateMetaInfoList, true);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			serializationProxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		serializationProxy =
				new KeyedBackendSerializationProxy<>(Thread.currentThread().getContextClassLoader());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertTrue(serializationProxy.isUsingKeyGroupCompression());
		Assert.assertTrue(serializationProxy.getKeySerializerConfigSnapshot() instanceof IntSerializer.IntSerializerSnapshot);

		assertEqualStateMetaInfoSnapshotsLists(stateMetaInfoList, serializationProxy.getStateMetaInfoSnapshots());
	}

	@Test
	public void testKeyedStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		StateMetaInfoSnapshot metaInfo = new RegisteredKeyValueStateBackendMetaInfo<>(
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
	public void testOperatorBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> keySerializer = DoubleSerializer.INSTANCE;
		TypeSerializer<?> valueSerializer = StringSerializer.INSTANCE;

		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>();

		stateMetaInfoSnapshots.add(new RegisteredOperatorStateBackendMetaInfo<>(
			"a", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE).snapshot());
		stateMetaInfoSnapshots.add(new RegisteredOperatorStateBackendMetaInfo<>(
			"b", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE).snapshot());
		stateMetaInfoSnapshots.add(new RegisteredOperatorStateBackendMetaInfo<>(
			"c", stateSerializer, OperatorStateHandle.Mode.UNION).snapshot());

		List<StateMetaInfoSnapshot> broadcastStateMetaInfoSnapshots = new ArrayList<>();

		broadcastStateMetaInfoSnapshots.add(new RegisteredBroadcastStateBackendMetaInfo<>(
				"d", OperatorStateHandle.Mode.BROADCAST, keySerializer, valueSerializer).snapshot());
		broadcastStateMetaInfoSnapshots.add(new RegisteredBroadcastStateBackendMetaInfo<>(
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
			new RegisteredOperatorStateBackendMetaInfo<>(
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

		RegisteredOperatorStateBackendMetaInfo<?> restoredMetaInfo =
			new RegisteredOperatorStateBackendMetaInfo<>(snapshot);

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
			new RegisteredBroadcastStateBackendMetaInfo<>(
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

		RegisteredBroadcastStateBackendMetaInfo<?, ?> restoredMetaInfo =
			new RegisteredBroadcastStateBackendMetaInfo<>(snapshot);

		Assert.assertEquals(name, restoredMetaInfo.getName());
		Assert.assertEquals(
			OperatorStateHandle.Mode.BROADCAST,
			restoredMetaInfo.getAssignmentMode());
		Assert.assertEquals(keySerializer, restoredMetaInfo.getKeySerializer());
		Assert.assertEquals(valueSerializer, restoredMetaInfo.getValueSerializer());
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
		Assert.assertEquals(
			expected.getSerializerSnapshotsImmutable(),
			actual.getSerializerSnapshotsImmutable());
	}
}
