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

import org.apache.flink.queryablestate.messages.KvStateRequest;
import org.apache.flink.queryablestate.messages.KvStateRequestFailure;
import org.apache.flink.queryablestate.messages.KvStateRequestResult;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.messages.MessageType;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.netty.message.KvStateSerializer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link KvStateSerializer}.
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

		ByteBuf buf = MessageSerializer.serializeKvStateRequest(
				alloc,
				requestId,
				kvStateId,
				serializedKeyAndNamespace);

		int frameLength = buf.readInt();
		assertEquals(MessageType.REQUEST, MessageSerializer.deserializeHeader(buf));
		KvStateRequest request = MessageSerializer.deserializeKvStateRequest(buf);
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

		ByteBuf buf = MessageSerializer.serializeKvStateRequest(
				alloc,
				1823,
				new KvStateID(),
				serializedKeyAndNamespace);

		int frameLength = buf.readInt();
		assertEquals(MessageType.REQUEST, MessageSerializer.deserializeHeader(buf));
		KvStateRequest request = MessageSerializer.deserializeKvStateRequest(buf);
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

		ByteBuf buf = MessageSerializer.serializeKvStateRequestResult(
				alloc,
				requestId,
				serializedResult);

		int frameLength = buf.readInt();
		assertEquals(MessageType.REQUEST_RESULT, MessageSerializer.deserializeHeader(buf));
		KvStateRequestResult request = MessageSerializer.deserializeKvStateRequestResult(buf);
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

		ByteBuf buf = MessageSerializer.serializeKvStateRequestResult(
				alloc,
				72727278,
				serializedResult);

		int frameLength = buf.readInt();

		assertEquals(MessageType.REQUEST_RESULT, MessageSerializer.deserializeHeader(buf));
		KvStateRequestResult request = MessageSerializer.deserializeKvStateRequestResult(buf);
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

		ByteBuf buf = MessageSerializer.serializeKvStateRequestFailure(
				alloc,
				requestId,
				cause);

		int frameLength = buf.readInt();
		assertEquals(MessageType.REQUEST_FAILURE, MessageSerializer.deserializeHeader(buf));
		KvStateRequestFailure request = MessageSerializer.deserializeKvStateRequestFailure(buf);
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

		ByteBuf buf = MessageSerializer.serializeServerFailure(alloc, cause);

		int frameLength = buf.readInt();
		assertEquals(MessageType.SERVER_FAILURE, MessageSerializer.deserializeHeader(buf));
		Throwable request = MessageSerializer.deserializeServerFailure(buf);
		assertEquals(buf.readerIndex(), frameLength + 4);

		assertEquals(cause.getClass(), request.getClass());
		assertEquals(cause.getMessage(), request.getMessage());
	}

	private byte[] randomByteArray(int capacity) {
		byte[] bytes = new byte[capacity];
		ThreadLocalRandom.current().nextBytes(bytes);
		return bytes;
	}
}
