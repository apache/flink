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

package org.apache.flink.runtime.query.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateRegistryListener;
import org.apache.flink.runtime.query.netty.message.KvStateRequestFailure;
import org.apache.flink.runtime.query.netty.message.KvStateRequestResult;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.query.netty.message.KvStateRequestType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KvStateServerHandlerTest extends TestLogger {

	/** Shared Thread pool for query execution */
	private final static ExecutorService TEST_THREAD_POOL = Executors.newSingleThreadExecutor();

	private final static int READ_TIMEOUT_MILLIS = 10000;

	@AfterClass
	public static void tearDown() throws Exception {
		if (TEST_THREAD_POOL != null) {
			TEST_THREAD_POOL.shutdown();
		}
	}

	/**
	 * Tests a simple successful query via an EmbeddedChannel.
	 */
	@Test
	public void testSimpleQuery() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateServerHandler handler = new KvStateServerHandler(registry, TEST_THREAD_POOL, stats);
		EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

		// Register state
		ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
		desc.setQueryable("vanilla");

		int numKeyGroups =1;
		AbstractStateBackend abstractBackend = new MemoryStateBackend();
		DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
		dummyEnv.setKvStateRegistry(registry);
		AbstractKeyedStateBackend<Integer> backend = abstractBackend.createKeyedStateBackend(
				dummyEnv,
				new JobID(),
				"test_op",
				IntSerializer.INSTANCE,
				numKeyGroups,
				new KeyGroupRange(0, 0),
				registry.createTaskRegistry(dummyEnv.getJobID(), dummyEnv.getJobVertexId()));

		final TestRegistryListener registryListener = new TestRegistryListener();
		registry.registerListener(registryListener);

		// Update the KvState and request it
		int expectedValue = 712828289;

		int key = 99812822;
		backend.setCurrentKey(key);
		ValueState<Integer> state = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				desc);

		state.update(expectedValue);

		byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
				key,
				IntSerializer.INSTANCE,
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE);

		long requestId = Integer.MAX_VALUE + 182828L;

		assertTrue(registryListener.registrationName.equals("vanilla"));

		ByteBuf request = KvStateRequestSerializer.serializeKvStateRequest(
				channel.alloc(),
				requestId,
				registryListener.kvStateId,
				serializedKeyAndNamespace);

		// Write the request and wait for the response
		channel.writeInbound(request);

		ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.REQUEST_RESULT, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequestResult response = KvStateRequestSerializer.deserializeKvStateRequestResult(buf);

		assertEquals(requestId, response.getRequestId());

		int actualValue = KvStateRequestSerializer.deserializeValue(response.getSerializedResult(), IntSerializer.INSTANCE);
		assertEquals(expectedValue, actualValue);

		assertEquals(stats.toString(), 1, stats.getNumRequests());

		// Wait for async successful request report
		long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
		while (stats.getNumSuccessful() != 1 && System.nanoTime() <= deadline) {
			Thread.sleep(10);
		}

		assertEquals(stats.toString(), 1, stats.getNumSuccessful());
	}

	/**
	 * Tests the failure response with {@link UnknownKvStateID} as cause on
	 * queries for unregistered KvStateIDs.
	 */
	@Test
	public void testQueryUnknownKvStateID() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateServerHandler handler = new KvStateServerHandler(registry, TEST_THREAD_POOL, stats);
		EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

		long requestId = Integer.MAX_VALUE + 182828L;
		ByteBuf request = KvStateRequestSerializer.serializeKvStateRequest(
				channel.alloc(),
				requestId,
				new KvStateID(),
				new byte[0]);

		// Write the request and wait for the response
		channel.writeInbound(request);

		ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.REQUEST_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequestFailure response = KvStateRequestSerializer.deserializeKvStateRequestFailure(buf);

		assertEquals(requestId, response.getRequestId());

		assertTrue("Did not respond with expected failure cause", response.getCause() instanceof UnknownKvStateID);

		assertEquals(1, stats.getNumRequests());
		assertEquals(1, stats.getNumFailed());
	}

	/**
	 * Tests the failure response with {@link UnknownKeyOrNamespace} as cause
	 * on queries for non-existing keys.
	 */
	@Test
	public void testQueryUnknownKey() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateServerHandler handler = new KvStateServerHandler(registry, TEST_THREAD_POOL, stats);
		EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

		int numKeyGroups = 1;
		AbstractStateBackend abstractBackend = new MemoryStateBackend();
		DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
		dummyEnv.setKvStateRegistry(registry);
		KeyedStateBackend<Integer> backend = abstractBackend.createKeyedStateBackend(
				dummyEnv,
				new JobID(),
				"test_op",
				IntSerializer.INSTANCE,
				numKeyGroups,
				new KeyGroupRange(0, 0),
				registry.createTaskRegistry(dummyEnv.getJobID(), dummyEnv.getJobVertexId()));

		final TestRegistryListener registryListener = new TestRegistryListener();
		registry.registerListener(registryListener);

		// Register state
		ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
		desc.setQueryable("vanilla");

		backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

		byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
				1238283,
				IntSerializer.INSTANCE,
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE);

		long requestId = Integer.MAX_VALUE + 22982L;

		assertTrue(registryListener.registrationName.equals("vanilla"));

		ByteBuf request = KvStateRequestSerializer.serializeKvStateRequest(
				channel.alloc(),
				requestId,
				registryListener.kvStateId,
				serializedKeyAndNamespace);

		// Write the request and wait for the response
		channel.writeInbound(request);

		ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.REQUEST_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequestFailure response = KvStateRequestSerializer.deserializeKvStateRequestFailure(buf);

		assertEquals(requestId, response.getRequestId());

		assertTrue("Did not respond with expected failure cause", response.getCause() instanceof UnknownKeyOrNamespace);

		assertEquals(1, stats.getNumRequests());
		assertEquals(1, stats.getNumFailed());
	}

	/**
	 * Tests the failure response on a failure on the {@link InternalKvState#getSerializedValue(byte[])}
	 * call.
	 */
	@Test
	public void testFailureOnGetSerializedValue() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateServerHandler handler = new KvStateServerHandler(registry, TEST_THREAD_POOL, stats);
		EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

		// Failing KvState
		InternalKvState<?> kvState = mock(InternalKvState.class);
		when(kvState.getSerializedValue(any(byte[].class)))
				.thenThrow(new RuntimeException("Expected test Exception"));

		KvStateID kvStateId = registry.registerKvState(
				new JobID(),
				new JobVertexID(),
				new KeyGroupRange(0, 0),
				"vanilla",
				kvState);

		ByteBuf request = KvStateRequestSerializer.serializeKvStateRequest(
				channel.alloc(),
				282872,
				kvStateId,
				new byte[0]);

		// Write the request and wait for the response
		channel.writeInbound(request);

		ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.REQUEST_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequestFailure response = KvStateRequestSerializer.deserializeKvStateRequestFailure(buf);

		assertTrue(response.getCause().getMessage().contains("Expected test Exception"));

		assertEquals(1, stats.getNumRequests());
		assertEquals(1, stats.getNumFailed());
	}

	/**
	 * Tests that the channel is closed if an Exception reaches the channel
	 * handler.
	 */
	@Test
	public void testCloseChannelOnExceptionCaught() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateServerHandler handler = new KvStateServerHandler(registry, TEST_THREAD_POOL, stats);
		EmbeddedChannel channel = new EmbeddedChannel(handler);

		channel.pipeline().fireExceptionCaught(new RuntimeException("Expected test Exception"));

		ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.SERVER_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		Throwable response = KvStateRequestSerializer.deserializeServerFailure(buf);

		assertTrue(response.getMessage().contains("Expected test Exception"));

		channel.closeFuture().await(READ_TIMEOUT_MILLIS);
		assertFalse(channel.isActive());
	}

	/**
	 * Tests the failure response on a rejected execution, because the query
	 * executor has been closed.
	 */
	@Test
	public void testQueryExecutorShutDown() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		ExecutorService closedExecutor = Executors.newSingleThreadExecutor();
		closedExecutor.shutdown();
		assertTrue(closedExecutor.isShutdown());

		KvStateServerHandler handler = new KvStateServerHandler(registry, closedExecutor, stats);
		EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

		int numKeyGroups = 1;
		AbstractStateBackend abstractBackend = new MemoryStateBackend();
		DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
		dummyEnv.setKvStateRegistry(registry);
		KeyedStateBackend<Integer> backend = abstractBackend.createKeyedStateBackend(
				dummyEnv,
				new JobID(),
				"test_op",
				IntSerializer.INSTANCE,
				numKeyGroups,
				new KeyGroupRange(0, 0),
				registry.createTaskRegistry(dummyEnv.getJobID(), dummyEnv.getJobVertexId()));

		final TestRegistryListener registryListener = new TestRegistryListener();
		registry.registerListener(registryListener);

		// Register state
		ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
		desc.setQueryable("vanilla");

		backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

		assertTrue(registryListener.registrationName.equals("vanilla"));

		ByteBuf request = KvStateRequestSerializer.serializeKvStateRequest(
				channel.alloc(),
				282872,
				registryListener.kvStateId,
				new byte[0]);

		// Write the request and wait for the response
		channel.writeInbound(request);

		ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.REQUEST_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequestFailure response = KvStateRequestSerializer.deserializeKvStateRequestFailure(buf);

		assertTrue(response.getCause().getMessage().contains("RejectedExecutionException"));

		assertEquals(1, stats.getNumRequests());
		assertEquals(1, stats.getNumFailed());
	}

	/**
	 * Tests response on unexpected messages.
	 */
	@Test
	public void testUnexpectedMessage() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateServerHandler handler = new KvStateServerHandler(registry, TEST_THREAD_POOL, stats);
		EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

		// Write the request and wait for the response
		ByteBuf unexpectedMessage = Unpooled.buffer(8);
		unexpectedMessage.writeInt(4);
		unexpectedMessage.writeInt(123238213);

		channel.writeInbound(unexpectedMessage);

		ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.SERVER_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		Throwable response = KvStateRequestSerializer.deserializeServerFailure(buf);

		assertEquals(0, stats.getNumRequests());
		assertEquals(0, stats.getNumFailed());

		unexpectedMessage = KvStateRequestSerializer.serializeKvStateRequestResult(
				channel.alloc(),
				192,
				new byte[0]);

		channel.writeInbound(unexpectedMessage);

		buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.SERVER_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		response = KvStateRequestSerializer.deserializeServerFailure(buf);

		assertTrue("Unexpected failure cause " + response.getClass().getName(), response instanceof IllegalArgumentException);

		assertEquals(0, stats.getNumRequests());
		assertEquals(0, stats.getNumFailed());
	}

	/**
	 * Tests that incoming buffer instances are recycled.
	 */
	@Test
	public void testIncomingBufferIsRecycled() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateServerHandler handler = new KvStateServerHandler(registry, TEST_THREAD_POOL, stats);
		EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

		ByteBuf request = KvStateRequestSerializer.serializeKvStateRequest(
				channel.alloc(),
				282872,
				new KvStateID(),
				new byte[0]);

		assertEquals(1, request.refCnt());

		// Write regular request
		channel.writeInbound(request);
		assertEquals("Buffer not recycled", 0, request.refCnt());

		// Write unexpected msg
		ByteBuf unexpected = channel.alloc().buffer(8);
		unexpected.writeInt(4);
		unexpected.writeInt(4);

		assertEquals(1, unexpected.refCnt());

		channel.writeInbound(unexpected);
		assertEquals("Buffer not recycled", 0, unexpected.refCnt());
	}

	/**
	 * Tests the failure response if the serializers don't match.
	 */
	@Test
	public void testSerializerMismatch() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateServerHandler handler = new KvStateServerHandler(registry, TEST_THREAD_POOL, stats);
		EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

		int numKeyGroups = 1;
		AbstractStateBackend abstractBackend = new MemoryStateBackend();
		DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
		dummyEnv.setKvStateRegistry(registry);
		AbstractKeyedStateBackend<Integer> backend = abstractBackend.createKeyedStateBackend(
				dummyEnv,
				new JobID(),
				"test_op",
				IntSerializer.INSTANCE,
				numKeyGroups,
				new KeyGroupRange(0, 0),
				registry.createTaskRegistry(dummyEnv.getJobID(), dummyEnv.getJobVertexId()));

		final TestRegistryListener registryListener = new TestRegistryListener();
		registry.registerListener(registryListener);

		// Register state
		ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
		desc.setQueryable("vanilla");

		ValueState<Integer> state = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				desc);

		int key = 99812822;

		// Update the KvState
		backend.setCurrentKey(key);
		state.update(712828289);

		byte[] wrongKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
				"wrong-key-type",
				StringSerializer.INSTANCE,
				"wrong-namespace-type",
				StringSerializer.INSTANCE);

		byte[] wrongNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
				key,
				IntSerializer.INSTANCE,
				"wrong-namespace-type",
				StringSerializer.INSTANCE);

		assertTrue(registryListener.registrationName.equals("vanilla"));
		ByteBuf request = KvStateRequestSerializer.serializeKvStateRequest(
				channel.alloc(),
				182828,
				registryListener.kvStateId,
				wrongKeyAndNamespace);

		// Write the request and wait for the response
		channel.writeInbound(request);

		ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.REQUEST_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		KvStateRequestFailure response = KvStateRequestSerializer.deserializeKvStateRequestFailure(buf);
		assertEquals(182828, response.getRequestId());
		assertTrue(response.getCause().getMessage().contains("IOException"));

		// Repeat with wrong namespace only
		request = KvStateRequestSerializer.serializeKvStateRequest(
				channel.alloc(),
				182829,
				registryListener.kvStateId,
				wrongNamespace);

		// Write the request and wait for the response
		channel.writeInbound(request);

		buf = (ByteBuf) readInboundBlocking(channel);
		buf.skipBytes(4); // skip frame length

		// Verify the response
		assertEquals(KvStateRequestType.REQUEST_FAILURE, KvStateRequestSerializer.deserializeHeader(buf));
		response = KvStateRequestSerializer.deserializeKvStateRequestFailure(buf);
		assertEquals(182829, response.getRequestId());
		assertTrue(response.getCause().getMessage().contains("IOException"));

		assertEquals(2, stats.getNumRequests());
		assertEquals(2, stats.getNumFailed());
	}

	/**
	 * Tests that large responses are chunked.
	 */
	@Test
	public void testChunkedResponse() throws Exception {
		KvStateRegistry registry = new KvStateRegistry();
		KvStateRequestStats stats = new AtomicKvStateRequestStats();

		KvStateServerHandler handler = new KvStateServerHandler(registry, TEST_THREAD_POOL, stats);
		EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

		int numKeyGroups = 1;
		AbstractStateBackend abstractBackend = new MemoryStateBackend();
		DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
		dummyEnv.setKvStateRegistry(registry);
		AbstractKeyedStateBackend<Integer> backend = abstractBackend.createKeyedStateBackend(
				dummyEnv,
				new JobID(),
				"test_op",
				IntSerializer.INSTANCE,
				numKeyGroups,
				new KeyGroupRange(0, 0),
				registry.createTaskRegistry(dummyEnv.getJobID(), dummyEnv.getJobVertexId()));

		final TestRegistryListener registryListener = new TestRegistryListener();
		registry.registerListener(registryListener);

		// Register state
		ValueStateDescriptor<byte[]> desc = new ValueStateDescriptor<>("any", BytePrimitiveArraySerializer.INSTANCE);
		desc.setQueryable("vanilla");

		ValueState<byte[]> state = backend.getPartitionedState(
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE,
				desc);

		// Update KvState
		byte[] bytes = new byte[2 * channel.config().getWriteBufferHighWaterMark()];

		byte current = 0;
		for (int i = 0; i < bytes.length; i++) {
			bytes[i] = current++;
		}

		int key = 99812822;
		backend.setCurrentKey(key);
		state.update(bytes);

		// Request
		byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
				key,
				IntSerializer.INSTANCE,
				VoidNamespace.INSTANCE,
				VoidNamespaceSerializer.INSTANCE);

		long requestId = Integer.MAX_VALUE + 182828L;

		assertTrue(registryListener.registrationName.equals("vanilla"));

		ByteBuf request = KvStateRequestSerializer.serializeKvStateRequest(
				channel.alloc(),
				requestId,
				registryListener.kvStateId,
				serializedKeyAndNamespace);

		// Write the request and wait for the response
		channel.writeInbound(request);

		Object msg = readInboundBlocking(channel);
		assertTrue("Not ChunkedByteBuf", msg instanceof ChunkedByteBuf);
	}

	// ------------------------------------------------------------------------

	/**
	 * Queries the embedded channel for data.
	 */
	private Object readInboundBlocking(EmbeddedChannel channel) throws InterruptedException, TimeoutException {
		final int sleepMillis = 50;

		int sleptMillis = 0;

		Object msg = null;
		while (sleptMillis < READ_TIMEOUT_MILLIS &&
				(msg = channel.readOutbound()) == null) {

			Thread.sleep(sleepMillis);
			sleptMillis += sleepMillis;
		}

		if (msg == null) {
			throw new TimeoutException();
		} else {
			return msg;
		}
	}

	/**
	 * Frame length decoder (expected by the serialized messages).
	 */
	private ChannelHandler getFrameDecoder() {
		return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4);
	}

	/**
	 * A listener that keeps the last updated KvState information so that a test
	 * can retrieve it.
	 */
	static class TestRegistryListener implements KvStateRegistryListener {
		volatile JobVertexID jobVertexID;
		volatile KeyGroupRange keyGroupIndex;
		volatile String registrationName;
		volatile KvStateID kvStateId;

		@Override
		public void notifyKvStateRegistered(JobID jobId,
				JobVertexID jobVertexId,
				KeyGroupRange keyGroupRange,
				String registrationName,
				KvStateID kvStateId) {
			this.jobVertexID = jobVertexId;
			this.keyGroupIndex = keyGroupRange;
			this.registrationName = registrationName;
			this.kvStateId = kvStateId;
		}

		@Override
		public void notifyKvStateUnregistered(JobID jobId,
				JobVertexID jobVertexId,
				KeyGroupRange keyGroupRange,
				String registrationName) {

		}
	}
}
