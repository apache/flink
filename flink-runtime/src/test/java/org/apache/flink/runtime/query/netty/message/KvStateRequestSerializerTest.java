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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.query.KvStateID;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class KvStateRequestSerializerTest {

	private final ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;

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
	 * Tests list serialization utils.
	 */
	@Test
	public void testListSerialization() throws Exception {
		TypeSerializer<Long> valueSerializer = LongSerializer.INSTANCE;

		// List
		int numElements = 10;

		List<Long> expectedValues = new ArrayList<>();
		for (int i = 0; i < numElements; i++) {
			expectedValues.add(ThreadLocalRandom.current().nextLong());
		}

		byte[] serializedValues = KvStateRequestSerializer.serializeList(expectedValues, valueSerializer);
		List<Long> actualValues = KvStateRequestSerializer.deserializeList(serializedValues, valueSerializer);
		assertEquals(expectedValues, actualValues);

		// Single value
		long expectedValue = ThreadLocalRandom.current().nextLong();
		byte[] serializedValue = KvStateRequestSerializer.serializeValue(expectedValue, valueSerializer);
		List<Long> actualValue = KvStateRequestSerializer.deserializeList(serializedValue, valueSerializer);
		assertEquals(1, actualValue.size());
		assertEquals(expectedValue, actualValue.get(0).longValue());
	}

	private byte[] randomByteArray(int capacity) {
		byte[] bytes = new byte[capacity];
		ThreadLocalRandom.current().nextBytes(bytes);
		return bytes;
	}
}
