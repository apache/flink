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

package org.apache.flink.runtime.query.netty.message;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;

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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link KvStateRequestSerializer}.
 */
@RunWith(Parameterized.class)
public class KvStateRequestSerializerTest {

	private final ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;

	@Parameterized.Parameters
	public static Collection<Boolean> parameters() {
		return Arrays.asList(false, true);
	}

	@Parameterized.Parameter
	public boolean async;

	/**
	 * Tests KvState request serialization.
	 */
	@Test
	public void testKvStateRequestSerialization() throws Exception {
		long requestId = Integer.MAX_VALUE + 1337L;
		KvStateID kvStateId = new KvStateID();
		byte[] serializedKeyAndNamespace = randomByteArray(1024);

		ByteBuf buf = KvStateRequestSerializer.serializeKvStateRequest(
				alloc,
				requestId,
				kvStateId,
				serializedKeyAndNamespace);

		int frameLength = buf.readInt();
		assertEquals(KvStateRequestType.REQUEST, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequest request = KvStateRequestSerializer.deserializeKvStateRequest(buf);
		assertEquals(buf.readerIndex(), frameLength + 4);

		assertEquals(requestId, request.getRequestId());
		assertEquals(kvStateId, request.getKvStateId());
		assertArrayEquals(serializedKeyAndNamespace, request.getSerializedKeyAndNamespace());
	}

	/**
	 * Tests KvState request serialization with zero-length serialized key and namespace.
	 */
	@Test
	public void testKvStateRequestSerializationWithZeroLengthKeyAndNamespace() throws Exception {
		byte[] serializedKeyAndNamespace = new byte[0];

		ByteBuf buf = KvStateRequestSerializer.serializeKvStateRequest(
				alloc,
				1823,
				new KvStateID(),
				serializedKeyAndNamespace);

		int frameLength = buf.readInt();
		assertEquals(KvStateRequestType.REQUEST, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequest request = KvStateRequestSerializer.deserializeKvStateRequest(buf);
		assertEquals(buf.readerIndex(), frameLength + 4);

		assertArrayEquals(serializedKeyAndNamespace, request.getSerializedKeyAndNamespace());
	}

	/**
	 * Tests that we don't try to be smart about <code>null</code> key and namespace.
	 * They should be treated explicitly.
	 */
	@Test(expected = NullPointerException.class)
	public void testNullPointerExceptionOnNullSerializedKeyAndNamepsace() throws Exception {
		new KvStateRequest(0, new KvStateID(), null);
	}

	/**
	 * Tests KvState request result serialization.
	 */
	@Test
	public void testKvStateRequestResultSerialization() throws Exception {
		long requestId = Integer.MAX_VALUE + 72727278L;
		byte[] serializedResult = randomByteArray(1024);

		ByteBuf buf = KvStateRequestSerializer.serializeKvStateRequestResult(
				alloc,
				requestId,
				serializedResult);

		int frameLength = buf.readInt();
		assertEquals(KvStateRequestType.REQUEST_RESULT, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequestResult request = KvStateRequestSerializer.deserializeKvStateRequestResult(buf);
		assertEquals(buf.readerIndex(), frameLength + 4);

		assertEquals(requestId, request.getRequestId());

		assertArrayEquals(serializedResult, request.getSerializedResult());
	}

	/**
	 * Tests KvState request result serialization with zero-length serialized result.
	 */
	@Test
	public void testKvStateRequestResultSerializationWithZeroLengthSerializedResult() throws Exception {
		byte[] serializedResult = new byte[0];

		ByteBuf buf = KvStateRequestSerializer.serializeKvStateRequestResult(
				alloc,
				72727278,
				serializedResult);

		int frameLength = buf.readInt();

		assertEquals(KvStateRequestType.REQUEST_RESULT, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequestResult request = KvStateRequestSerializer.deserializeKvStateRequestResult(buf);
		assertEquals(buf.readerIndex(), frameLength + 4);

		assertArrayEquals(serializedResult, request.getSerializedResult());
	}

	/**
	 * Tests that we don't try to be smart about <code>null</code> results.
	 * They should be treated explicitly.
	 */
	@Test(expected = NullPointerException.class)
	public void testNullPointerExceptionOnNullSerializedResult() throws Exception {
		new KvStateRequestResult(0, null);
	}

	/**
	 * Tests KvState request failure serialization.
	 */
	@Test
	public void testKvStateRequestFailureSerialization() throws Exception {
		long requestId = Integer.MAX_VALUE + 1111222L;
		IllegalStateException cause = new IllegalStateException("Expected test");

		ByteBuf buf = KvStateRequestSerializer.serializeKvStateRequestFailure(
				alloc,
				requestId,
				cause);

		int frameLength = buf.readInt();
		assertEquals(KvStateRequestType.REQUEST_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequestFailure request = KvStateRequestSerializer.deserializeKvStateRequestFailure(buf);
		assertEquals(buf.readerIndex(), frameLength + 4);

		assertEquals(requestId, request.getRequestId());
		assertEquals(cause.getClass(), request.getCause().getClass());
		assertEquals(cause.getMessage(), request.getCause().getMessage());
	}

	/**
	 * Tests KvState server failure serialization.
	 */
	@Test
	public void testServerFailureSerialization() throws Exception {
		IllegalStateException cause = new IllegalStateException("Expected test");

		ByteBuf buf = KvStateRequestSerializer.serializeServerFailure(alloc, cause);

		int frameLength = buf.readInt();
		assertEquals(KvStateRequestType.SERVER_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		Throwable request = KvStateRequestSerializer.deserializeServerFailure(buf);
		assertEquals(buf.readerIndex(), frameLength + 4);

		assertEquals(cause.getClass(), request.getClass());
		assertEquals(cause.getMessage(), request.getMessage());
	}

	/**
	 * Tests key and namespace serialization utils.
	 */
	@Test
	public void testKeyAndNamespaceSerialization() throws Exception {
		TypeSerializer<Long> keySerializer = LongSerializer.INSTANCE;
		TypeSerializer<String> namespaceSerializer = StringSerializer.INSTANCE;

		long expectedKey = Integer.MAX_VALUE + 12323L;
		String expectedNamespace = "knilf";

		byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
				expectedKey, keySerializer, expectedNamespace, namespaceSerializer);

		Tuple2<Long, String> actual = KvStateRequestSerializer.deserializeKeyAndNamespace(
				serializedKeyAndNamespace, keySerializer, namespaceSerializer);

		assertEquals(expectedKey, actual.f0.longValue());
		assertEquals(expectedNamespace, actual.f1);
	}

	/**
	 * Tests key and namespace deserialization utils with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testKeyAndNamespaceDeserializationEmpty() throws Exception {
		KvStateRequestSerializer.deserializeKeyAndNamespace(
			new byte[] {}, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
	}

	/**
	 * Tests key and namespace deserialization utils with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testKeyAndNamespaceDeserializationTooShort() throws Exception {
		KvStateRequestSerializer.deserializeKeyAndNamespace(
			new byte[] {1}, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
	}

	/**
	 * Tests key and namespace deserialization utils with too many bytes.
	 */
	@Test(expected = IOException.class)
	public void testKeyAndNamespaceDeserializationTooMany1() throws Exception {
		// Long + null String + 1 byte
		KvStateRequestSerializer.deserializeKeyAndNamespace(
			new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 42, 0, 2}, LongSerializer.INSTANCE,
			StringSerializer.INSTANCE);
	}

	/**
	 * Tests key and namespace deserialization utils with too many bytes.
	 */
	@Test(expected = IOException.class)
	public void testKeyAndNamespaceDeserializationTooMany2() throws Exception {
		// Long + null String + 2 bytes
		KvStateRequestSerializer.deserializeKeyAndNamespace(
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

		byte[] serializedValue = KvStateRequestSerializer.serializeValue(expectedValue, valueSerializer);
		long actualValue = KvStateRequestSerializer.deserializeValue(serializedValue, valueSerializer);

		assertEquals(expectedValue, actualValue);
	}

	/**
	 * Tests value deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeValueEmpty() throws Exception {
		KvStateRequestSerializer.deserializeValue(new byte[] {}, LongSerializer.INSTANCE);
	}

	/**
	 * Tests value deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeValueTooShort() throws Exception {
		// 1 byte (incomplete Long)
		KvStateRequestSerializer.deserializeValue(new byte[] {1}, LongSerializer.INSTANCE);
	}

	/**
	 * Tests value deserialization with too many bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeValueTooMany1() throws Exception {
		// Long + 1 byte
		KvStateRequestSerializer.deserializeValue(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 2},
			LongSerializer.INSTANCE);
	}

	/**
	 * Tests value deserialization with too many bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeValueTooMany2() throws Exception {
		// Long + 2 bytes
		KvStateRequestSerializer.deserializeValue(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 2, 2},
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
				new ExecutionConfig()
			);
		longHeapKeyedStateBackend.setCurrentKey(key);

		final InternalListState<VoidNamespace, Long> listState = longHeapKeyedStateBackend.createListState(
				VoidNamespaceSerializer.INSTANCE,
				new ListStateDescriptor<>("test", LongSerializer.INSTANCE));

		testListSerialization(key, listState);
	}

	/**
	 * Verifies that the serialization of a list using the given list state
	 * matches the deserialization with {@link KvStateRequestSerializer#deserializeList}.
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
			final InternalListState<VoidNamespace, Long> listState) throws Exception {

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
			KvStateRequestSerializer.serializeKeyAndNamespace(
				key, LongSerializer.INSTANCE,
				VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);

		final byte[] serializedValues = listState.getSerializedValue(serializedKey);

		List<Long> actualValues = KvStateRequestSerializer.deserializeList(serializedValues, valueSerializer);
		assertEquals(expectedValues, actualValues);

		// Single value
		long expectedValue = ThreadLocalRandom.current().nextLong();
		byte[] serializedValue = KvStateRequestSerializer.serializeValue(expectedValue, valueSerializer);
		List<Long> actualValue = KvStateRequestSerializer.deserializeList(serializedValue, valueSerializer);
		assertEquals(1, actualValue.size());
		assertEquals(expectedValue, actualValue.get(0).longValue());
	}

	/**
	 * Tests list deserialization with too few bytes.
	 */
	@Test
	public void testDeserializeListEmpty() throws Exception {
		List<Long> actualValue = KvStateRequestSerializer
			.deserializeList(new byte[] {}, LongSerializer.INSTANCE);
		assertEquals(0, actualValue.size());
	}

	/**
	 * Tests list deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeListTooShort1() throws Exception {
		// 1 byte (incomplete Long)
		KvStateRequestSerializer.deserializeList(new byte[] {1}, LongSerializer.INSTANCE);
	}

	/**
	 * Tests list deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeListTooShort2() throws Exception {
		// Long + 1 byte (separator) + 1 byte (incomplete Long)
		KvStateRequestSerializer.deserializeList(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 2, 3},
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
					new ExecutionConfig()
			);
		longHeapKeyedStateBackend.setCurrentKey(key);

		final InternalMapState<VoidNamespace, Long, String> mapState = (InternalMapState<VoidNamespace, Long, String>) longHeapKeyedStateBackend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				new MapStateDescriptor<>("test", LongSerializer.INSTANCE, StringSerializer.INSTANCE));

		testMapSerialization(key, mapState);
	}

	/**
	 * Verifies that the serialization of a map using the given map state
	 * matches the deserialization with {@link KvStateRequestSerializer#deserializeList}.
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
			final InternalMapState<VoidNamespace, Long, String> mapState) throws Exception {

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
			KvStateRequestSerializer.serializeKeyAndNamespace(
				key, LongSerializer.INSTANCE,
				VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE);

		final byte[] serializedValues = mapState.getSerializedValue(serializedKey);

		Map<Long, String> actualValues = KvStateRequestSerializer.deserializeMap(serializedValues, userKeySerializer, userValueSerializer);
		assertEquals(expectedValues.size(), actualValues.size());
		for (Map.Entry<Long, String> actualEntry : actualValues.entrySet()) {
			assertEquals(expectedValues.get(actualEntry.getKey()), actualEntry.getValue());
		}

		// Single value
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		long expectedKey = ThreadLocalRandom.current().nextLong();
		String expectedValue = Long.toString(expectedKey);
		byte[] isNull = {0};

		baos.write(KvStateRequestSerializer.serializeValue(expectedKey, userKeySerializer));
		baos.write(isNull);
		baos.write(KvStateRequestSerializer.serializeValue(expectedValue, userValueSerializer));
		byte[] serializedValue = baos.toByteArray();

		Map<Long, String> actualValue = KvStateRequestSerializer.deserializeMap(serializedValue, userKeySerializer, userValueSerializer);
		assertEquals(1, actualValue.size());
		assertEquals(expectedValue, actualValue.get(expectedKey));
	}

	/**
	 * Tests map deserialization with too few bytes.
	 */
	@Test
	public void testDeserializeMapEmpty() throws Exception {
		Map<Long, String> actualValue = KvStateRequestSerializer
			.deserializeMap(new byte[] {}, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
		assertEquals(0, actualValue.size());
	}

	/**
	 * Tests map deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeMapTooShort1() throws Exception {
		// 1 byte (incomplete Key)
		KvStateRequestSerializer.deserializeMap(new byte[] {1}, LongSerializer.INSTANCE, StringSerializer.INSTANCE);
	}

	/**
	 * Tests map deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeMapTooShort2() throws Exception {
		// Long (Key) + 1 byte (incomplete Value)
		KvStateRequestSerializer.deserializeMap(new byte[]{1, 1, 1, 1, 1, 1, 1, 1, 0},
				LongSerializer.INSTANCE, LongSerializer.INSTANCE);
	}

	/**
	 * Tests map deserialization with too few bytes.
	 */
	@Test(expected = IOException.class)
	public void testDeserializeMapTooShort3() throws Exception {
		// Long (Key1) + Boolean (false) + Long (Value1) + 1 byte (incomplete Key2)
		KvStateRequestSerializer.deserializeMap(new byte[] {1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 3},
			LongSerializer.INSTANCE, LongSerializer.INSTANCE);
	}

	private byte[] randomByteArray(int capacity) {
		byte[] bytes = new byte[capacity];
		ThreadLocalRandom.current().nextBytes(bytes);
		return bytes;
	}
}
