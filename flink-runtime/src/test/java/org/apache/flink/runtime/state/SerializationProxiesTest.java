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
	public void testSerializationRoundtrip() throws Exception {

		TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		List<KeyedBackendSerializationProxy.StateMetaInfo<?, ?>> stateMetaInfoList = new ArrayList<>();

		stateMetaInfoList.add(
				new KeyedBackendSerializationProxy.StateMetaInfo<>("a", namespaceSerializer, stateSerializer));
		stateMetaInfoList.add(
				new KeyedBackendSerializationProxy.StateMetaInfo<>("b", namespaceSerializer, stateSerializer));
		stateMetaInfoList.add(
				new KeyedBackendSerializationProxy.StateMetaInfo<>("c", namespaceSerializer, stateSerializer));

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
	public void testMetaInfoSerialization() throws Exception {

		String name = "test";
		TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
		TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

		KeyedBackendSerializationProxy.StateMetaInfo<?, ?> metaInfo =
				new KeyedBackendSerializationProxy.StateMetaInfo<>(name, namespaceSerializer, stateSerializer);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			metaInfo.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		metaInfo = new KeyedBackendSerializationProxy.StateMetaInfo<>(Thread.currentThread().getContextClassLoader());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			metaInfo.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(name, metaInfo.getName());

		Assert.assertEquals(
				namespaceSerializer.getCanonicalClassName(),
				metaInfo.getNamespaceSerializerSerializationProxy().getTypeSerializerClassName());

		Assert.assertEquals(
				stateSerializer.getCanonicalClassName(),
				metaInfo.getStateSerializerSerializationProxy().getTypeSerializerClassName());

		Assert.assertEquals(
				namespaceSerializer.getVersion(),
				metaInfo.getNamespaceSerializerSerializationProxy().getVersion());

		Assert.assertEquals(
				stateSerializer.getVersion(),
				metaInfo.getStateSerializerSerializationProxy().getVersion());
	}
}