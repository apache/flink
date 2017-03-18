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
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SerializationProxiesTest {

	@Test
	public void testKeyedBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<KeyedBackendSerializationProxy.StateMetaInfo<?, ?>> stateMetaInfoList = new ArrayList<>();

		stateMetaInfoList.add(
				new KeyedBackendSerializationProxy.StateMetaInfo<>(StateDescriptor.Type.VALUE, "a", namespaceSerializer, stateSerializer));
		stateMetaInfoList.add(
				new KeyedBackendSerializationProxy.StateMetaInfo<>(StateDescriptor.Type.VALUE, "b", namespaceSerializer, stateSerializer));
		stateMetaInfoList.add(
				new KeyedBackendSerializationProxy.StateMetaInfo<>(StateDescriptor.Type.VALUE, "c", namespaceSerializer, stateSerializer));

		KeyedBackendSerializationProxy serializationProxy =
				new KeyedBackendSerializationProxy(keySerializer, stateMetaInfoList);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			serializationProxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		serializationProxy =
				new KeyedBackendSerializationProxy(Thread.currentThread().getContextClassLoader());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			serializationProxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(keySerializer, serializationProxy.getKeySerializerProxy().getTypeSerializer());
		Assert.assertEquals(stateMetaInfoList, serializationProxy.getNamedStateSerializationProxies());
	}

	@Test
	public void testKeyedStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		KeyedBackendSerializationProxy.StateMetaInfo<?, ?> metaInfo =
				new KeyedBackendSerializationProxy.StateMetaInfo<>(StateDescriptor.Type.VALUE, name, namespaceSerializer, stateSerializer);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			metaInfo.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		metaInfo = new KeyedBackendSerializationProxy.StateMetaInfo<>(Thread.currentThread().getContextClassLoader());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			metaInfo.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(name, metaInfo.getStateName());
	}


	@Test
	public void testOperatorBackendSerializationProxyRoundtrip() throws Exception {

		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<OperatorBackendSerializationProxy.StateMetaInfo<?>> stateMetaInfoList = new ArrayList<>();

		stateMetaInfoList.add(
				new OperatorBackendSerializationProxy.StateMetaInfo<>("a", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
		stateMetaInfoList.add(
				new OperatorBackendSerializationProxy.StateMetaInfo<>("b", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
		stateMetaInfoList.add(
				new OperatorBackendSerializationProxy.StateMetaInfo<>("c", stateSerializer, OperatorStateHandle.Mode.BROADCAST));

		OperatorBackendSerializationProxy serializationProxy =
				new OperatorBackendSerializationProxy(stateMetaInfoList);

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

		Assert.assertEquals(stateMetaInfoList, serializationProxy.getNamedStateSerializationProxies());
	}

	@Test
	public void testOperatorStateMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		OperatorBackendSerializationProxy.StateMetaInfo<?> metaInfo =
				new OperatorBackendSerializationProxy.StateMetaInfo<>(name, stateSerializer, OperatorStateHandle.Mode.BROADCAST);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			metaInfo.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		metaInfo = new OperatorBackendSerializationProxy.StateMetaInfo<>(Thread.currentThread().getContextClassLoader());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			metaInfo.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(name, metaInfo.getName());
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