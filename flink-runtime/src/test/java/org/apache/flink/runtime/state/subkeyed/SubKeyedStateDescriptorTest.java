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

package org.apache.flink.runtime.state.subkeyed;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.testutils.CommonTestUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link SubKeyedStateDescriptor}s.
 */
public class SubKeyedStateDescriptorTest {

	@Test
	public void testSubKeyedValueStateDescriptor() throws Exception {

		try {
			new SubKeyedValueStateDescriptor<>(null, IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the state name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedValueStateDescriptor<>("testName", (TypeSerializer<Integer>) null,
				StringSerializer.INSTANCE, FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedValueStateDescriptor<>("testName", IntSerializer.INSTANCE,
				(TypeSerializer<String>) null, FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the namespace serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedValueStateDescriptor<>("testName", IntSerializer.INSTANCE,
				StringSerializer.INSTANCE, (TypeSerializer<Float>) null);
			fail("Should throw exceptions because the value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		TypeSerializer<String> serializer =
			new KryoSerializer<>(String.class, new ExecutionConfig());

		SubKeyedValueStateDescriptor<Integer, String, Float> descriptor =
			new SubKeyedValueStateDescriptor<>("testName",
				IntSerializer.INSTANCE, serializer, FloatSerializer.INSTANCE);
		assertEquals("testName", descriptor.getName());
		assertEquals(IntSerializer.INSTANCE, descriptor.getKeySerializer());
		assertEquals(serializer, descriptor.getNamespaceSerializer());
		assertEquals(FloatSerializer.INSTANCE, descriptor.getValueSerializer());

		SubKeyedValueStateDescriptor<Integer, String, Float> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
	}

	@Test
	public void testSubKeyedListStateDescriptor() throws Exception {

		try {
			new SubKeyedListStateDescriptor<>(null,
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedListStateDescriptor<>("testName",
				(TypeSerializer<Integer>) null, StringSerializer.INSTANCE,
				FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedListStateDescriptor<>("testName",
				IntSerializer.INSTANCE, (TypeSerializer<String>) null,
				FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the namespace serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedListStateDescriptor<>("testName",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				(TypeSerializer<Float>) null);
			fail("Should throw exceptions because the value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		TypeSerializer<String> namespaceSerializer =
			new KryoSerializer<>(String.class, new ExecutionConfig());

		SubKeyedListStateDescriptor<Integer, String, Float> descriptor =
			new SubKeyedListStateDescriptor<>("testName",
				IntSerializer.INSTANCE, namespaceSerializer, FloatSerializer.INSTANCE);
		assertEquals("testName", descriptor.getName());
		assertEquals(IntSerializer.INSTANCE, descriptor.getKeySerializer());
		assertEquals(namespaceSerializer, descriptor.getNamespaceSerializer());
		assertEquals(FloatSerializer.INSTANCE, descriptor.getElementSerializer());

		SubKeyedListStateDescriptor<Integer, String, Float> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
	}

	@Test
	public void testSubKeyedMapStateDescriptor() throws Exception {

		try {
			new SubKeyedMapStateDescriptor<>(null,
				(TypeSerializer<Integer>) null, StringSerializer.INSTANCE,
				LongSerializer.INSTANCE, FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedMapStateDescriptor<>("testName",
				(TypeSerializer<Integer>) null, StringSerializer.INSTANCE,
				LongSerializer.INSTANCE, FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, (TypeSerializer<String>) null,
				LongSerializer.INSTANCE, FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the namespace serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				(TypeSerializer<Long>) null, FloatSerializer.INSTANCE);
			fail("Should throw exceptions because the map key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, StringSerializer.INSTANCE,
				LongSerializer.INSTANCE, (TypeSerializer<Float>) null);
			fail("Should throw exceptions because the map value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		ExecutionConfig executionConfig = new ExecutionConfig();
		TypeSerializer<String> mapKeySerializer =
			new KryoSerializer<>(String.class, executionConfig);
		TypeSerializer<Float> mapValueSerializer =
			new KryoSerializer<>(Float.class, executionConfig);

		SubKeyedMapStateDescriptor<Integer, Long, String, Float> descriptor =
			new SubKeyedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, LongSerializer.INSTANCE, mapKeySerializer, mapValueSerializer);
		assertEquals("testName", descriptor.getName());
		assertEquals(IntSerializer.INSTANCE, descriptor.getKeySerializer());
		assertEquals(LongSerializer.INSTANCE, descriptor.getNamespaceSerializer());
		assertEquals(mapKeySerializer, descriptor.getMapKeySerializer());
		assertEquals(mapValueSerializer, descriptor.getMapValueSerializer());

		SubKeyedMapStateDescriptor<Integer, Long, String, Float> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
	}

	@Test
	public void testSubKeyedSortedMapStateDescriptor() throws Exception {

		Comparator<String> comparator = new NaturalComparator<>();

		try {
			new SubKeyedSortedMapStateDescriptor<>(null,
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE,
				comparator, StringSerializer.INSTANCE, LongSerializer.INSTANCE);
			fail("Should throw exceptions because the name is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedSortedMapStateDescriptor<>("testName",
				(TypeSerializer<Integer>) null, FloatSerializer.INSTANCE,
				comparator, StringSerializer.INSTANCE, LongSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedSortedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, (TypeSerializer<Float>) null,
				comparator, StringSerializer.INSTANCE, LongSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedSortedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE,
				null, StringSerializer.INSTANCE, LongSerializer.INSTANCE);
			fail("Should throw exceptions because the comparator is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedSortedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE,
				comparator, null, LongSerializer.INSTANCE);
			fail("Should throw exceptions because the map key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SubKeyedSortedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, FloatSerializer.INSTANCE,
				comparator, StringSerializer.INSTANCE, (TypeSerializer<Long>) null);
			fail("Should throw exceptions because the map value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		ExecutionConfig executionConfig = new ExecutionConfig();
		TypeSerializer<String> mapKeySerializer =
			new KryoSerializer<>(String.class, executionConfig);
		TypeSerializer<Float> mapValueSerializer =
			new KryoSerializer<>(Float.class, executionConfig);

		SubKeyedSortedMapStateDescriptor<Integer, Long, String, Float> descriptor =
			new SubKeyedSortedMapStateDescriptor<>("testName",
				IntSerializer.INSTANCE, LongSerializer.INSTANCE,
				comparator, mapKeySerializer, mapValueSerializer);
		assertEquals("testName", descriptor.getName());
		assertEquals(IntSerializer.INSTANCE, descriptor.getKeySerializer());
		assertEquals(LongSerializer.INSTANCE, descriptor.getNamespaceSerializer());
		assertEquals(comparator, descriptor.getComparator());
		assertEquals(mapKeySerializer, descriptor.getMapKeySerializer());
		assertEquals(mapValueSerializer, descriptor.getMapValueSerializer());

		SubKeyedSortedMapStateDescriptor<Integer, Long, String, Float> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
	}
}

