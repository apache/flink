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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Unit tests for the {@link IdentitySerializerIndex}.
 */
public class IdentitySerializerIndexTest {

	@Test
	public void testNonDuplicatedSerializerIndexing() {
		final IdentitySerializerIndex serializerIndex = new IdentitySerializerIndex();

		// -- test that the same serializer instances should be maintained as a single entry

		// using stateless serializers; duplicate should return the same serializer
		serializerIndex.index(IntSerializer.INSTANCE);
		serializerIndex.index(IntSerializer.INSTANCE.duplicate());

		Assert.assertEquals(1, serializerIndex.getSize());
		Assert.assertTrue(serializerIndex.containsSerializer(IntSerializer.INSTANCE));

		// -- test that the different serializer instances should be maintained as separate entries

		// use a stateful serializer and duplicate it to create a separate serializer
		TypeSerializer<Object> serializer = TypeExtractor.getForClass(Object.class).createSerializer(new ExecutionConfig());
		TypeSerializer<Object> otherSerializer = serializer.duplicate();

		// make sure our assumption is correct
		Assert.assertTrue(serializer != otherSerializer);

		serializerIndex.index(serializer);
		serializerIndex.index(otherSerializer);

		Assert.assertEquals(3, serializerIndex.getSize());
		Assert.assertTrue(serializerIndex.containsSerializer(serializer));
		Assert.assertTrue(serializerIndex.containsSerializer(otherSerializer));
	}

	@Test
	public void testSerializerIndexSerializationRoundtrip() throws IOException {
		IdentitySerializerIndex serializerIndex = new IdentitySerializerIndex();

		TypeSerializer<?> serializer1 = IntSerializer.INSTANCE;
		TypeSerializer<?> serializer2 = TypeExtractor.getForClass(Object.class).createSerializer(new ExecutionConfig());
		TypeSerializer<?> serializer3 = serializer2.duplicate();

		serializerIndex.index(serializer1);
		serializerIndex.index(serializer2);
		serializerIndex.index(serializer3);

		int index1 = serializerIndex.getIndexOf(serializer1);
		int index2 = serializerIndex.getIndexOf(serializer2);
		int index3 = serializerIndex.getIndexOf(serializer3);

		// serializer to bytes
		byte[] bytes;
		try (
			ByteArrayOutputStream buffer = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper bufferWrapper = new DataOutputViewStreamWrapper(buffer)) {

			serializerIndex.write(bufferWrapper);
			bytes = buffer.toByteArray();
		}

		// ... and then deserialize again
		try (
			ByteArrayInputStream buffer = new ByteArrayInputStream(bytes);
			DataInputViewStreamWrapper bufferWrapper = new DataInputViewStreamWrapper(buffer)) {

			serializerIndex = new IdentitySerializerIndex(Thread.currentThread().getContextClassLoader());
			serializerIndex.read(bufferWrapper);
		}

		Assert.assertEquals(3, serializerIndex.getSize());
		Assert.assertEquals(serializer1, serializerIndex.getSerializer(index1));
		Assert.assertEquals(serializer2, serializerIndex.getSerializer(index2));
		Assert.assertEquals(serializer3, serializerIndex.getSerializer(index3));
	}
}
