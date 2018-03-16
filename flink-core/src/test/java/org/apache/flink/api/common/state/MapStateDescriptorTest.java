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

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MapStateDescriptorTest {
	
	@Test
	public void testMapStateDescriptorEagerSerializer() throws Exception {

		TypeSerializer<Integer> keySerializer = new KryoSerializer<>(Integer.class, new ExecutionConfig());
		TypeSerializer<String> valueSerializer = new KryoSerializer<>(String.class, new ExecutionConfig());
		
		MapStateDescriptor<Integer, String> descr = 
				new MapStateDescriptor<>("testName", keySerializer, valueSerializer);
		
		assertEquals("testName", descr.getName());
		assertNotNull(descr.getSerializer());
		assertTrue(descr.getSerializer() instanceof MapSerializer);
		assertNotNull(descr.getKeySerializer());
		assertEquals(keySerializer, descr.getKeySerializer());
		assertNotNull(descr.getValueSerializer());
		assertEquals(valueSerializer, descr.getValueSerializer());

		MapStateDescriptor<Integer, String> copy = CommonTestUtils.createCopySerializable(descr);

		assertEquals("testName", copy.getName());
		assertNotNull(copy.getSerializer());
		assertTrue(copy.getSerializer() instanceof MapSerializer);

		assertNotNull(copy.getKeySerializer());
		assertEquals(keySerializer, copy.getKeySerializer());
		assertNotNull(copy.getValueSerializer());
		assertEquals(valueSerializer, copy.getValueSerializer());
	}

	@Test
	public void testMapStateDescriptorLazySerializer() throws Exception {
		// some different registered value
		ExecutionConfig cfg = new ExecutionConfig();
		cfg.registerKryoType(TaskInfo.class);

		MapStateDescriptor<Path, String> descr =
				new MapStateDescriptor<>("testName", Path.class, String.class);
		
		try {
			descr.getSerializer();
			fail("should cause an exception");
		} catch (IllegalStateException ignored) {}

		descr.initializeSerializerUnlessSet(cfg);

		assertNotNull(descr.getSerializer());
		assertTrue(descr.getSerializer() instanceof MapSerializer);

		assertNotNull(descr.getKeySerializer());
		assertTrue(descr.getKeySerializer() instanceof KryoSerializer);

		assertTrue(((KryoSerializer<?>) descr.getKeySerializer()).getKryo().getRegistration(TaskInfo.class).getId() > 0);
		
		assertNotNull(descr.getValueSerializer());
		assertTrue(descr.getValueSerializer() instanceof StringSerializer);
	}

	@Test
	public void testMapStateDescriptorAutoSerializer() throws Exception {

		MapStateDescriptor<String, Long> descr =
				new MapStateDescriptor<>("testName", String.class, Long.class);

		MapStateDescriptor<String, Long> copy = CommonTestUtils.createCopySerializable(descr);

		assertEquals("testName", copy.getName());

		assertNotNull(copy.getSerializer());
		assertTrue(copy.getSerializer() instanceof MapSerializer);

		assertNotNull(copy.getKeySerializer());
		assertEquals(StringSerializer.INSTANCE, copy.getKeySerializer());
		assertNotNull(copy.getValueSerializer());
		assertEquals(LongSerializer.INSTANCE, copy.getValueSerializer());
	}

	/**
	 * FLINK-6775
	 *
	 * Tests that the returned serializer is duplicated. This allows to
	 * share the state descriptor.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testSerializerDuplication() {
		TypeSerializer<String> keySerializer = mock(TypeSerializer.class);
		TypeSerializer<Long> valueSerializer = mock(TypeSerializer.class);
		when(keySerializer.duplicate()).thenAnswer(new Answer<TypeSerializer<String>>() {
			@Override
			public TypeSerializer<String> answer(InvocationOnMock invocation) throws Throwable {
				return mock(TypeSerializer.class);
			}
		});
		when(valueSerializer.duplicate()).thenAnswer(new Answer<TypeSerializer<Long>>() {
			@Override
			public TypeSerializer<Long> answer(InvocationOnMock invocation) throws Throwable {
				return mock(TypeSerializer.class);
			}
		});

		MapStateDescriptor<String, Long> descr = new MapStateDescriptor<>("foobar", keySerializer, valueSerializer);

		TypeSerializer<String> keySerializerA = descr.getKeySerializer();
		TypeSerializer<String> keySerializerB = descr.getKeySerializer();
		TypeSerializer<Long> valueSerializerA = descr.getValueSerializer();
		TypeSerializer<Long> valueSerializerB = descr.getValueSerializer();

		// check that we did not retrieve the same serializers
		assertNotSame(keySerializerA, keySerializerB);
		assertNotSame(valueSerializerA, valueSerializerB);

		TypeSerializer<Map<String, Long>> serializerA = descr.getSerializer();
		TypeSerializer<Map<String, Long>> serializerB = descr.getSerializer();

		assertNotSame(serializerA, serializerB);
	}
}
