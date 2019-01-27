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
import org.apache.flink.api.common.functions.Comparator;
import org.apache.flink.api.common.functions.NaturalComparator;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.SortedMapSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.SortedMapTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link SortedMapStateDescriptor}.
 */
public class SortedMapStateDescriptorTest extends TestLogger {

	@Test
	public void testConstructorWithTypeSerializer() throws Exception {

		try {
			new SortedMapStateDescriptor<>("testName",
				null,
				IntSerializer.INSTANCE, StringSerializer.INSTANCE);
			fail("Should throw exceptions because the comparator is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SortedMapStateDescriptor<>("testName",
				new NaturalComparator<Integer>(),
				null, StringSerializer.INSTANCE);
			fail("Should throw exceptions because the key serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SortedMapStateDescriptor<>("testName",
				new NaturalComparator<>(),
				IntSerializer.INSTANCE, (TypeSerializer<String>) null);
			fail("Should throw exceptions because the value serializer is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		ExecutionConfig executionConfig = new ExecutionConfig();
		TypeSerializer<Integer> keySerializer =
			new KryoSerializer<>(Integer.class, executionConfig);
		TypeSerializer<String> valueSerializer =
			new KryoSerializer<>(String.class, executionConfig);

		SortedMapStateDescriptor<Integer, String> descriptor =
			new SortedMapStateDescriptor<>("testName",
				new NaturalComparator<>(), keySerializer, valueSerializer);
		assertEquals("testName", descriptor.getName());
		assertNull(descriptor.getTypeInformation());
		assertTrue(descriptor.getSerializer() instanceof SortedMapSerializer);

		SortedMapSerializer<Integer, String> sortedMapSerializer = descriptor.getSerializer();
		assertEquals(keySerializer, sortedMapSerializer.getKeySerializer());
		assertEquals(valueSerializer, sortedMapSerializer.getValueSerializer());
		assertEquals(new NaturalComparator<>(), sortedMapSerializer.getComparator());

		SortedMapStateDescriptor<Integer, String> descriptorOther1 =
			new SortedMapStateDescriptor<>("testName",
				new NaturalComparator<>(),
				IntSerializer.INSTANCE, valueSerializer);
		assertNotEquals(descriptor, descriptorOther1);

		SortedMapStateDescriptor<Integer, String> descriptorOther2 =
			new SortedMapStateDescriptor<>("testName",
				new NaturalComparator<>(),
				keySerializer, StringSerializer.INSTANCE);
		assertNotEquals(descriptor, descriptorOther2);

		SortedMapStateDescriptor<Integer, String> descriptorOther3 =
			new SortedMapStateDescriptor<>("testName",
				(Comparator<Integer>) ((o1, o2) -> (o2 - o1)),
				keySerializer, StringSerializer.INSTANCE);
		assertNotEquals(descriptor, descriptorOther3);

		SortedMapStateDescriptor<Integer, String> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
		assertEquals("testName", descriptorCopy.getName());
		assertEquals(sortedMapSerializer, descriptorCopy.getSerializer());
	}

	@Test
	public void testConstructorWithTypeInformation() throws Exception {

		try {
			new SortedMapStateDescriptor<>("testName",
				null,
				BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
			fail("Should throw exceptions because the comparator is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SortedMapStateDescriptor<>("testName",
				new NaturalComparator<Integer>(),
				null, BasicTypeInfo.STRING_TYPE_INFO);
			fail("Should throw exceptions because the key type information is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SortedMapStateDescriptor<>("testName",
				new NaturalComparator<>(),
				BasicTypeInfo.INT_TYPE_INFO, (TypeInformation<String>) null);
			fail("Should throw exceptions because the value type information is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		// some different registered value
		ExecutionConfig cfg = new ExecutionConfig();
		cfg.registerKryoType(TaskInfo.class);

		SortedMapStateDescriptor<Integer, Path> descriptor =
			new SortedMapStateDescriptor<>(
				"testName",
				new NaturalComparator<>(),
				BasicTypeInfo.INT_TYPE_INFO,
				new GenericTypeInfo<>(Path.class)
			);
		boolean throwException = false;
		try {
			descriptor.getSerializer();
		} catch (IllegalStateException e) {
			throwException = true;
		}
		assertTrue(throwException);
		assertTrue(descriptor.getTypeInformation() instanceof SortedMapTypeInfo);

		descriptor.initializeSerializerUnlessSet(cfg);
		SortedMapSerializer<Integer, Path> sortedMapSerializer = descriptor.getSerializer();
		Comparator<Integer> comparator = sortedMapSerializer.getComparator();
		assertEquals(new NaturalComparator<>(), comparator);
		TypeSerializer<Integer> keySerializer = sortedMapSerializer.getKeySerializer();
		assertTrue(keySerializer instanceof IntSerializer);
		TypeSerializer<Path> valueSerializer = sortedMapSerializer.getValueSerializer();
		assertTrue(valueSerializer instanceof KryoSerializer);
		KryoSerializer<Path> kryoSerializer = (KryoSerializer<Path>) valueSerializer;
		assertTrue(kryoSerializer.getKryo().getRegistration(TaskInfo.class).getId() > 0);

		SortedMapStateDescriptor<Integer, Path> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
		assertEquals("testName", descriptorCopy.getName());
		assertEquals(sortedMapSerializer, descriptorCopy.getSerializer());
	}

	@Test
	public void testConstructorWithClass() throws Exception {

		try {
			new SortedMapStateDescriptor<>("testName",
				null, Integer.class, String.class);
			fail("Should throw exceptions because the comparator is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SortedMapStateDescriptor<>("testName",
				new NaturalComparator<Integer>(), null, String.class);
			fail("Should throw exceptions because the key class is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		try {
			new SortedMapStateDescriptor<>("testName",
				new NaturalComparator<>(), Integer.class, (Class<String>) null);
			fail("Should throw exceptions because the value class is null.");
		} catch (Exception e) {
			assertTrue(e instanceof NullPointerException);
		}

		// some different registered value
		ExecutionConfig cfg = new ExecutionConfig();
		cfg.registerKryoType(TaskInfo.class);

		SortedMapStateDescriptor<Integer, Path> descriptor =
			new SortedMapStateDescriptor<>(
				"testName",
				new NaturalComparator<>(),
				Integer.class,
				Path.class
			);
		boolean throwException = false;
		try {
			descriptor.getSerializer();
		} catch (IllegalStateException e) {
			throwException = true;
		}
		assertTrue(throwException);
		assertTrue(descriptor.getTypeInformation() instanceof SortedMapTypeInfo);

		descriptor.initializeSerializerUnlessSet(cfg);
		SortedMapSerializer<Integer, Path> sortedMapSerializer = descriptor.getSerializer();
		Comparator<Integer> comparator = sortedMapSerializer.getComparator();
		assertEquals(new NaturalComparator<>(), comparator);
		TypeSerializer<Integer> keySerializer = sortedMapSerializer.getKeySerializer();
		assertTrue(keySerializer instanceof IntSerializer);
		TypeSerializer<Path> valueSerializer = sortedMapSerializer.getValueSerializer();
		assertTrue(valueSerializer instanceof KryoSerializer);
		KryoSerializer<Path> kryoSerializer = (KryoSerializer<Path>) valueSerializer;
		assertTrue(kryoSerializer.getKryo().getRegistration(TaskInfo.class).getId() > 0);

		SortedMapStateDescriptor<Integer, Path> descriptorCopy =
			CommonTestUtils.createCopySerializable(descriptor);
		assertEquals(descriptor, descriptorCopy);
		assertEquals("testName", descriptorCopy.getName());
		assertEquals(sortedMapSerializer, descriptorCopy.getSerializer());
	}
}
