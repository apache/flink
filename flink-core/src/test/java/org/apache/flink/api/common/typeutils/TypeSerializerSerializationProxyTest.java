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
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.Assert.fail;

public class TypeSerializerSerializationProxyTest {

	@Test
	public void testStateSerializerSerializationProxy() throws Exception {

		TypeSerializer<?> serializer = IntSerializer.INSTANCE;

		TypeSerializerSerializationProxy<?> proxy = new TypeSerializerSerializationProxy<>(serializer);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			proxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		proxy = new TypeSerializerSerializationProxy<>(Thread.currentThread().getContextClassLoader());

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			proxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertEquals(serializer, proxy.getTypeSerializer());
	}

	@Test
	public void testStateSerializerSerializationProxyClassNotFound() throws Exception {

		TypeSerializer<?> serializer = IntSerializer.INSTANCE;

		TypeSerializerSerializationProxy<?> proxy = new TypeSerializerSerializationProxy<>(serializer);

		byte[] serialized;
		try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
			proxy.write(new DataOutputViewStreamWrapper(out));
			serialized = out.toByteArray();
		}

		proxy = new TypeSerializerSerializationProxy<>(new URLClassLoader(new URL[0], null));

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			proxy.read(new DataInputViewStreamWrapper(in));
			fail("ClassNotFoundException expected, leading to IOException");
		} catch (IOException expected) {

		}

		proxy = new TypeSerializerSerializationProxy<>(new URLClassLoader(new URL[0], null), true);

		try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
			proxy.read(new DataInputViewStreamWrapper(in));
		}

		Assert.assertTrue(proxy.getTypeSerializer() instanceof TypeSerializerSerializationProxy.ClassNotFoundDummyTypeSerializer);

		Assert.assertArrayEquals(
				InstantiationUtil.serializeObject(serializer),
				((TypeSerializerSerializationProxy.ClassNotFoundDummyTypeSerializer<?>) proxy.getTypeSerializer()).getActualBytes());
	}
}