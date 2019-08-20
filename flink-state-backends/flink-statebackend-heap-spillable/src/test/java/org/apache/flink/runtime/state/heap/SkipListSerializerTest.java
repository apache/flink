/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link SkipListKeySerializer}.
 */
public class SkipListSerializerTest {

	@Test
	public void testSkipListKeySerializer() throws IOException {
		TypeSerializer<String> keySerializer = StringSerializer.INSTANCE;
		TypeSerializer<String> namespaceSerializer = StringSerializer.INSTANCE;

		SkipListKeySerializer<String, String> skipListKeySerializer =
			new SkipListKeySerializer<>(keySerializer, namespaceSerializer);

		for (int i = 0; i < 10; i++) {
			String key = "key-abcdedg" + i;
			String namespace = "namespace-dfsfdafd" + i;

			byte[] skipListKey = skipListKeySerializer.serialize(key, namespace);
			int offset = 10;
			byte[] data = new byte[10 + skipListKey.length];
			System.arraycopy(skipListKey, 0, data, offset, skipListKey.length);
			ByteBuffer skipListKeyByteBuffer = ByteBuffer.wrap(data);
			assertEquals(key, skipListKeySerializer.deserializeKey(skipListKeyByteBuffer, offset, skipListKey.length));
			assertEquals(namespace, skipListKeySerializer.deserializeNamespace(skipListKeyByteBuffer, offset, skipListKey.length));

			Tuple2<byte[], byte[]> serializedKeyAndNamespace =
				skipListKeySerializer.getSerializedKeyAndNamespace(skipListKeyByteBuffer, offset);
			assertEquals(key, deserialize(keySerializer, serializedKeyAndNamespace.f0));
			assertEquals(namespace, deserialize(namespaceSerializer, serializedKeyAndNamespace.f1));

			byte[] serializedNamespace = skipListKeySerializer.serializeNamespace(namespace);
			assertEquals(namespace, deserialize(namespaceSerializer, serializedNamespace));
		}
	}

	@Test
	public void testSkipListValueSerializer() throws IOException {
		TypeSerializer<String> stateSerializer = StringSerializer.INSTANCE;
		SkipListValueSerializer<String> skipListValueSerializer =
			new SkipListValueSerializer<>(stateSerializer);

		for (int i = 0; i < 10; i++) {
			String state = "value-" + i;
			byte[] value = skipListValueSerializer.serialize(state);
			int offset = 10;
			byte[] data = new byte[10 + value.length];
			System.arraycopy(value, 0, data, offset, value.length);
			assertEquals(state, deserialize(stateSerializer, value));
			assertEquals(state, skipListValueSerializer.deserializeState(ByteBuffer.wrap(data), offset, value.length));
		}
	}

	private <T> T deserialize(TypeSerializer<T> serializer, byte[] data) throws IOException {
		ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(data);
		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);
		return serializer.deserialize(inputView);
	}
}
