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

package org.apache.flink.queryablestate.network;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.VoidNamespace;
import org.apache.flink.queryablestate.client.VoidNamespaceSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link KvStateSerializer}.
 */
@RunWith(Parameterized.class)
public class KvStateRequestSerializerTest {

	@Parameterized.Parameters
	public static Collection<Boolean> parameters() {
		return Arrays.asList(false, true);
	}

	@Parameterized.Parameter
	public boolean async;

	/**
	 * Tests key and namespace serialization utils.
	 */
	@Test
	public void testKeyAndNamespaceSerialization() throws Exception {
		TypeSerializer<Long> keySerializer = LongSerializer.INSTANCE;
		TypeSerializer<String> namespaceSerializer = StringSerializer.INSTANCE;

		long expectedKey = Integer.MAX_VALUE + 12323L;
		String expectedNamespace = "knilf";

		byte[] serializedKeyAndNamespace = KvStateSerializer.serializeKeyAndNamespace(
				expectedKey, keySerializer, expectedNamespace, namespaceSerializer);

		Tuple2<Long, String> actual = KvStateSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace, keySerializer, namespaceSerializer);

		assertEquals(expectedKey, actual.f0.longValue());
		assertEquals(expectedNamespace, actual.f1);
	}

	/**
	 * Tests key and namespace deserialization utils with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testKeyAndNamespaceDeserializationEmpty() throws Exception {
		KvStateSerializer.deserializeKeyAndNamespace(
			new byte[] {}, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
	}

	/**
	 * Tests key and namespace deserialization utils with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testKeyAndNamespaceDeserializationTooShort() throws Exception {
		KvStateSerializer.deserializeKeyAndNamespace(
			new byte[] {1}, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
	}

	/**
	 * Tests key and namespace deserialization utils with too many bytes.
	 */
	@Test(expected = IOException.class)
	public void testKeyAndNamespaceDeserializationTooMany1() throws Exception {
		// Long + null String + 1 byte
		KvStateSerializer.deserializeKeyAndNamespace(
			new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 42, 0, 2}, LongSerializer.INSTANCE,
			StringSerializer.INSTANCE);
	}

	/**
	 * Tests key and namespace deserialization utils with too many bytes.
	 */
	@Test(expected = IOException.class)
	public void testKeyAndNamespaceDeserializationTooMany2() throws Exception {
		// Long + null String + 2 bytes
		KvStateSerializer.deserializeKeyAndNamespace(
			new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 42, 0, 2, 2}, LongSerializer.INSTANCE,
			StringSerializer.INSTANCE);
	}

	/**
	 * Tests value serialization utils.
	 */
	@Test
	public void testValueSerialization() throws Exception {
		TypeSerializer<Long> valueSerializer = LongSerializer.INSTANCE;
		long expectedValue = Long.MAX_VALUE - 1292929292L;

		byte[] serializedValue = KvStateSerializer.serializeValue(expectedValue, valueSerializer);
		long actualValue = KvStateSerializer.deserializeValue(serializedValue, valueSerializer);

		assertEquals(expectedValue, actualValue);
	}

	/**
	 * Tests value deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeValueEmpty() throws Exception {
		KvStateSerializer.deserializeValue(new byte[] {}, LongSerializer.INSTANCE);
	}

	/**
	 * Tests value deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeValueTooShort() throws Exception {
		// 1 byte (incomplete Long)
		KvStateSerializer.deserializeValue(new byte[] {1}, LongSerializer.INSTANCE);
	}

	/**
	 * Tests value deserialization with too many bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeValueTooMany1() throws Exception {
		// Long + 1 byte
		KvStateSerializer.deserializeValue(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 2},
			LongSerializer.INSTANCE);
	}

	/**
	 * Tests value deserialization with too many bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeValueTooMany2() throws Exception {
		// Long + 2 bytes
		KvStateSerializer.deserializeValue(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 2, 2},
			LongSerializer.INSTANCE);
	}

	/**
	 * Tests list serialization utils.
	 */
	@Test
	public void testListSerialization() throws Exception {
		final long key = 0L;

		// objects for heap state list serialisation
		final HeapKeyedStateBackend<Long> longHeapKeyedStateBackend =
			new HeapKeyedStateBackend<>(
				mock(TaskKvStateRegistry.class),
				LongSerializer.INSTANCE,
				ClassLoader.getSystemClassLoader(),
				1,
				new KeyGroupRange(0, 0),
				async,
				new ExecutionConfig(),
				TestLocalRecoveryConfig.disabled()
			);
		longHeapKeyedStateBackend.setCurrentKey(key);

		final InternalListState<Long, VoidNamespace, Long> listState = longHeapKeyedStateBackend.createListState(
				VoidNamespaceSerializer.INSTANCE,
				new ListStateDescriptor<>("test", LongSerializer.INSTANCE));

		testListSerialization(key, listState);
	}

	/**
	 * Verifies that the serialization of a list using the given list state
	 * matches the deserialization with {@link KvStateSerializer#deserializeList}.
	 *
	 * @param key
	 * 		key of the list state
	 * @param listState
	 * 		list state using the {@link VoidNamespace}, must also be a {@link InternalKvState} instance
	 *
	 * @throws Exception
	 */
	public static void testListSerialization(
			final long key,
			final InternalListState<Long, VoidNamespace, Long> listState) throws Exception {

		TypeSerializer<Long> valueSerializer = LongSerializer.INSTANCE;
		listState.setCurrentNamespace(VoidNamespace.INSTANCE);

		// List
		final int numElements = 10;

		final List<Long> expectedValues = new ArrayList<>();
		for (int i = 0; i < numElements; i++) {
			final long value = ThreadLocalRandom.current().nextLong();
			expectedValues.add(value);
			listState.add(value);
		}

		final byte[] serializedKey =
			KvStateSerializer.serializeKeyAndNamespace(
				key, LongSerializer.INSTANCE,
				VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);

		final byte[] serializedValues = listState.getSerializedValue(
				serializedKey,
				listState.getKeySerializer(),
				listState.getNamespaceSerializer(),
				listState.getValueSerializer());

		List<Long> actualValues = KvStateSerializer.deserializeList(serializedValues, valueSerializer);
		assertEquals(expectedValues, actualValues);

		// Single value
		long expectedValue = ThreadLocalRandom.current().nextLong();
		byte[] serializedValue = KvStateSerializer.serializeValue(expectedValue, valueSerializer);
		List<Long> actualValue = KvStateSerializer.deserializeList(serializedValue, valueSerializer);
		assertEquals(1, actualValue.size());
		assertEquals(expectedValue, actualValue.get(0).longValue());
	}

	/**
	 * Tests list deserialization with too few bytes.
	 */
	@Test
	public void testDeserializeListEmpty() throws Exception {
		List<Long> actualValue = KvStateSerializer
			.deserializeList(new byte[] {}, LongSerializer.INSTANCE);
		assertEquals(0, actualValue.size());
	}

	/**
	 * Tests list deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeListTooShort1() throws Exception {
		// 1 byte (incomplete Long)
		KvStateSerializer.deserializeList(new byte[] {1}, LongSerializer.INSTANCE);
	}

	/**
	 * Tests list deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeListTooShort2() throws Exception {
		// Long + 1 byte (separator) + 1 byte (incomplete Long)
		KvStateSerializer.deserializeList(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 2, 3},
			LongSerializer.INSTANCE);
	}

	/**
	 * Tests map serialization utils.
	 */
	@Test
	public void testMapSerialization() throws Exception {
		final long key = 0L;

		// objects for heap state list serialisation
		final HeapKeyedStateBackend<Long> longHeapKeyedStateBackend =
			new HeapKeyedStateBackend<>(
				mock(TaskKvStateRegistry.class),
				LongSerializer.INSTANCE,
				ClassLoader.getSystemClassLoader(),
				1,
				new KeyGroupRange(0, 0),
				async,
				new ExecutionConfig(),
				TestLocalRecoveryConfig.disabled()
			);
		longHeapKeyedStateBackend.setCurrentKey(key);

		final InternalMapState<Long, VoidNamespace, Long, String> mapState =
				(InternalMapState<Long, VoidNamespace, Long, String>)
						longHeapKeyedStateBackend.getPartitionedState(
								VoidNamespace.INSTANCE,
								VoidNamespaceSerializer.INSTANCE,
								new MapStateDescriptor<>("test", LongSerializer.INSTANCE, StringSerializer.INSTANCE));

		testMapSerialization(key, mapState);
	}

	/**
	 * Verifies that the serialization of a map using the given map state
	 * matches the deserialization with {@link KvStateSerializer#deserializeList}.
	 *
	 * @param key
	 * 		key of the map state
	 * @param mapState
	 * 		map state using the {@link VoidNamespace}, must also be a {@link InternalKvState} instance
	 *
	 * @throws Exception
	 */
	public static void testMapSerialization(
			final long key,
			final InternalMapState<Long, VoidNamespace, Long, String> mapState) throws Exception {

		TypeSerializer<Long> userKeySerializer = LongSerializer.INSTANCE;
		TypeSerializer<String> userValueSerializer = StringSerializer.INSTANCE;
		mapState.setCurrentNamespace(VoidNamespace.INSTANCE);

		// Map
		final int numElements = 10;

		final Map<Long, String> expectedValues = new HashMap<>();
		for (int i = 1; i <= numElements; i++) {
			final long value = ThreadLocalRandom.current().nextLong();
			expectedValues.put(value, Long.toString(value));
			mapState.put(value, Long.toString(value));
		}

		expectedValues.put(0L, null);
		mapState.put(0L, null);

		final byte[] serializedKey =
			KvStateSerializer.serializeKeyAndNamespace(
				key, LongSerializer.INSTANCE,
				VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);

		final byte[] serializedValues = mapState.getSerializedValue(
				serializedKey,
				mapState.getKeySerializer(),
				mapState.getNamespaceSerializer(),
				mapState.getValueSerializer());

		Map<Long, String> actualValues = KvStateSerializer.deserializeMap(serializedValues, userKeySerializer, userValueSerializer);
		assertEquals(expectedValues.size(), actualValues.size());
		for (Map.Entry<Long, String> actualEntry : actualValues.entrySet()) {
			assertEquals(expectedValues.get(actualEntry.getKey()), actualEntry.getValue());
		}

		// Single value
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		long expectedKey = ThreadLocalRandom.current().nextLong();
		String expectedValue = Long.toString(expectedKey);
		byte[] isNull = {0};

		baos.write(KvStateSerializer.serializeValue(expectedKey, userKeySerializer));
		baos.write(isNull);
		baos.write(KvStateSerializer.serializeValue(expectedValue, userValueSerializer));
		byte[] serializedValue = baos.toByteArray();

		Map<Long, String> actualValue = KvStateSerializer.deserializeMap(serializedValue, userKeySerializer, userValueSerializer);
		assertEquals(1, actualValue.size());
		assertEquals(expectedValue, actualValue.get(expectedKey));
	}

	/**
	 * Tests map deserialization with too few bytes.
	 */
	@Test
	public void testDeserializeMapEmpty() throws Exception {
		Map<Long, String> actualValue = KvStateSerializer
			.deserializeMap(new byte[] {}, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
		assertEquals(0, actualValue.size());
	}

	/**
	 * Tests map deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeMapTooShort1() throws Exception {
		// 1 byte (incomplete Key)
		KvStateSerializer.deserializeMap(new byte[] {1}, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
	}

	/**
	 * Tests map deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeMapTooShort2() throws Exception {
		// Long (Key) + 1 byte (incomplete Value)
		KvStateSerializer.deserializeMap(new byte[]{1, 1, 1, 1, 1, 1, 1, 1, 0},
				LongSerializer.INSTANCE, LongSerializer.INSTANCE);
	}

	/**
	 * Tests map deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeMapTooShort3() throws Exception {
		// Long (Key1) + Boolean (false) + Long (Value1) + 1 byte (incomplete Key2)
		KvStateSerializer.deserializeMap(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 3},
			LongSerializer.INSTANCE, LongSerializer.INSTANCE);
	}

	private byte[] randomByteArray(int capacity) {
		byte[] bytes = new byte[capacity];
		ThreadLocalRandom.current().nextBytes(bytes);
		return bytes;
	}
}
