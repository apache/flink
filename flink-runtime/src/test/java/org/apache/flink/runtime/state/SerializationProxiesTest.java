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
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TypeSerializerSerializationUtil.class)
public class SerializationProxiesTest {

	@Test
	public void testKeyedBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoList = new ArrayList<>();

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
				new KeyedBackendSerializationProxy<>(Thread.currentThread().getContextClassLoader());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(true, serializationProxy.isUsingKeyGroupCompression());
		Assert.assertEquals(keySerializer, serializationProxy.getKeySerializer());
		Assert.assertEquals(keySerializer.snapshotConfiguration(), serializationProxy.getKeySerializerConfigSnapshot());
		Assert.assertEquals(stateMetaInfoList, serializationProxy.getStateMetaInfoSnapshots());
	}

	@Test
	public void testKeyedBackendSerializationProxyRoundtripWithSerializerSerializationFailures() throws Exception {

		TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?>> stateMetaInfoList = new ArrayList<>();

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
			new KeyedBackendSerializationProxy<>(Thread.currentThread().getContextClassLoader());

		// mock failure when deserializing serializers
		TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<?> mockProxy =
				mock(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class);
		doThrow(new IOException()).when(mockProxy).read(any(DataInputViewStreamWrapper.class));
		PowerMockito.whenNew(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class).withAnyArguments().thenReturn(mockProxy);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(true, serializationProxy.isUsingKeyGroupCompression());
		Assert.assertEquals(null, serializationProxy.getKeySerializer());
		Assert.assertEquals(keySerializer.snapshotConfiguration(), serializationProxy.getKeySerializerConfigSnapshot());

		for (RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> meta : serializationProxy.getStateMetaInfoSnapshots()) {
			Assert.assertEquals(null, meta.getNamespaceSerializer());
			Assert.assertEquals(null, meta.getStateSerializer());
			Assert.assertEquals(namespaceSerializer.snapshotConfiguration(), meta.getNamespaceSerializerConfigSnapshot());
			Assert.assertEquals(stateSerializer.snapshotConfiguration(), meta.getStateSerializerConfigSnapshot());
		}
	}

	@Test
	public void testKeyedStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> metaInfo = new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, name, namespaceSerializer, stateSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			KeyedBackendStateMetaInfoSnapshotReaderWriters
				.getWriterForVersion(KeyedBackendSerializationProxy.VERSION, metaInfo)
				.writeStateMetaInfo(new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			metaInfo = KeyedBackendStateMetaInfoSnapshotReaderWriters
				.getReaderForVersion(KeyedBackendSerializationProxy.VERSION, Thread.currentThread().getContextClassLoader())
				.readStateMetaInfo(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(name, metaInfo.getName());
	}

	@Test
	public void testKeyedStateMetaInfoReadSerializerFailureResilience() throws Exception {
		String name = "test";
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		RegisteredKeyedBackendStateMetaInfo.Snapshot<?, ?> metaInfo = new RegisteredKeyedBackendStateMetaInfo<>(
			StateDescriptor.Type.VALUE, name, namespaceSerializer, stateSerializer).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			KeyedBackendStateMetaInfoSnapshotReaderWriters
				.getWriterForVersion(KeyedBackendSerializationProxy.VERSION, metaInfo)
				.writeStateMetaInfo(new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		// mock failure when deserializing serializer
		TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<?> mockProxy =
				mock(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class);
		doThrow(new IOException()).when(mockProxy).read(any(DataInputViewStreamWrapper.class));
		PowerMockito.whenNew(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class).withAnyArguments().thenReturn(mockProxy);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			metaInfo = KeyedBackendStateMetaInfoSnapshotReaderWriters
				.getReaderForVersion(KeyedBackendSerializationProxy.VERSION, Thread.currentThread().getContextClassLoader())
				.readStateMetaInfo(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(name, metaInfo.getName());
		Assert.assertEquals(null, metaInfo.getNamespaceSerializer());
		Assert.assertEquals(null, metaInfo.getStateSerializer());
		Assert.assertEquals(namespaceSerializer.snapshotConfiguration(), metaInfo.getNamespaceSerializerConfigSnapshot());
		Assert.assertEquals(stateSerializer.snapshotConfiguration(), metaInfo.getStateSerializerConfigSnapshot());
	}

	@Test
	public void testOperatorBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> stateMetaInfoSnapshots = new ArrayList<>();

		stateMetaInfoSnapshots.add(new RegisteredOperatorBackendStateMetaInfo<>(
			"a", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE).snapshot());
		stateMetaInfoSnapshots.add(new RegisteredOperatorBackendStateMetaInfo<>(
			"b", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE).snapshot());
		stateMetaInfoSnapshots.add(new RegisteredOperatorBackendStateMetaInfo<>(
			"c", stateSerializer, OperatorStateHandle.Mode.BROADCAST).snapshot());

		OperatorBackendSerializationProxy serializationProxy =
				new OperatorBackendSerializationProxy(stateMetaInfoSnapshots);

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

		Assert.assertEquals(stateMetaInfoSnapshots, serializationProxy.getStateMetaInfoSnapshots());
	}

	@Test
	public void testOperatorStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		RegisteredOperatorBackendStateMetaInfo.Snapshot<?> metaInfo =
			new RegisteredOperatorBackendStateMetaInfo<>(
				name, stateSerializer, OperatorStateHandle.Mode.BROADCAST).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			OperatorBackendStateMetaInfoSnapshotReaderWriters
				.getWriterForVersion(OperatorBackendSerializationProxy.VERSION, metaInfo)
				.writeStateMetaInfo(new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			metaInfo = OperatorBackendStateMetaInfoSnapshotReaderWriters
				.getReaderForVersion(OperatorBackendSerializationProxy.VERSION, Thread.currentThread().getContextClassLoader())
				.readStateMetaInfo(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(name, metaInfo.getName());
	}

	@Test
	public void testOperatorStateMetaInfoReadSerializerFailureResilience() throws Exception {
		String name = "test";
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		RegisteredOperatorBackendStateMetaInfo.Snapshot<?> metaInfo =
			new RegisteredOperatorBackendStateMetaInfo<>(
				name, stateSerializer, OperatorStateHandle.Mode.BROADCAST).snapshot();

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			OperatorBackendStateMetaInfoSnapshotReaderWriters
				.getWriterForVersion(OperatorBackendSerializationProxy.VERSION, metaInfo)
				.writeStateMetaInfo(new DataOutputViewStreamWrapper(out));

			serialized = out.toByteArray();
		}

		// mock failure when deserializing serializer
		TypeSerializerSerializationUtil.TypeSerializerSerializationProxy<?> mockProxy =
				mock(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class);
		doThrow(new IOException()).when(mockProxy).read(any(DataInputViewStreamWrapper.class));
		PowerMockito.whenNew(TypeSerializerSerializationUtil.TypeSerializerSerializationProxy.class).withAnyArguments().thenReturn(mockProxy);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			metaInfo = OperatorBackendStateMetaInfoSnapshotReaderWriters
				.getReaderForVersion(OperatorBackendSerializationProxy.VERSION, Thread.currentThread().getContextClassLoader())
				.readStateMetaInfo(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(name, metaInfo.getName());
		Assert.assertEquals(null, metaInfo.getPartitionStateSerializer());
		Assert.assertEquals(stateSerializer.snapshotConfiguration(), metaInfo.getPartitionStateSerializerConfigSnapshot());
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
}
