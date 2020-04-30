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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link MapStateDescriptor}.
 */
public class MapStateDescriptorTest {

	@Test
	public void testMapStateDescriptor() throws Exception {

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
	public void testHashCodeEquals() throws Exception {
		final String name = "testName";

		MapStateDescriptor<String, String> original = new MapStateDescriptor<>(name, String.class, String.class);
		MapStateDescriptor<String, String> same = new MapStateDescriptor<>(name, String.class, String.class);
		MapStateDescriptor<String, String> sameBySerializer =
				new MapStateDescriptor<>(name, StringSerializer.INSTANCE, StringSerializer.INSTANCE);

		// test that hashCode() works on state descriptors with initialized and uninitialized serializers
		assertEquals(original.hashCode(), same.hashCode());
		assertEquals(original.hashCode(), sameBySerializer.hashCode());

		assertEquals(original, same);
		assertEquals(original, sameBySerializer);

		// equality with a clone
		MapStateDescriptor<String, String> clone = CommonTestUtils.createCopySerializable(original);
		assertEquals(original, clone);

		// equality with an initialized
		clone.initializeSerializerUnlessSet(new ExecutionConfig());
		assertEquals(original, clone);

		original.initializeSerializerUnlessSet(new ExecutionConfig());
		assertEquals(original, same);
	}

	/**
	 * FLINK-6775.
	 *
	 * <p>Tests that the returned serializer is duplicated. This allows to
	 * share the state descriptor.
	 */
	@Test
	public void testSerializerDuplication() {
		// we need a serializer that actually duplicates for testing (a stateful one)
		// we use Kryo here, because it meets these conditions
		TypeSerializer<String> keySerializer = new KryoSerializer<>(String.class, new ExecutionConfig());
		TypeSerializer<Long> valueSerializer = new KryoSerializer<>(Long.class, new ExecutionConfig());

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
