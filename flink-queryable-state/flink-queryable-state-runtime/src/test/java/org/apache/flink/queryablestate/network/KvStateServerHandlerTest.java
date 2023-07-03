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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.client.VoidNamespace;
import org.apache.flink.queryablestate.client.VoidNamespaceSerializer;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateIdException;
import org.apache.flink.queryablestate.messages.KvStateInternalRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.messages.MessageType;
import org.apache.flink.queryablestate.network.messages.RequestFailure;
import org.apache.flink.queryablestate.network.stats.AtomicKvStateRequestStats;
import org.apache.flink.queryablestate.network.stats.DisabledKvStateRequestStats;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.queryablestate.server.KvStateServerHandler;
import org.apache.flink.queryablestate.server.KvStateServerImpl;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateRegistryListener;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KvStateServerHandler}. */
@Disabled(
        "KvStateServerHandlerTest is unstable. See FLINK-13553 for more information. Since the community "
                + "does not have time to work on QS, we decided to temporarily ignore this test case in order"
                + "to maintain build stability.")
class KvStateServerHandlerTest {

    private static KvStateServerImpl testServer;

    private static final long READ_TIMEOUT_MILLIS = 10000L;

    @BeforeAll
    static void setup() {
        try {
            testServer =
                    new KvStateServerImpl(
                            InetAddress.getLocalHost().getHostName(),
                            Collections.singletonList(0).iterator(),
                            1,
                            1,
                            new KvStateRegistry(),
                            new DisabledKvStateRequestStats());
            testServer.start();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        testServer.shutdown();
    }

    /** Tests a simple successful query via an EmbeddedChannel. */
    @Test
    void testSimpleQuery() throws Exception {
        KvStateRegistry registry = new KvStateRegistry();
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(testServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

        // Register state
        ValueStateDescriptor<Integer> desc =
                new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
        desc.setQueryable("vanilla");

        int numKeyGroups = 1;
        AbstractStateBackend abstractBackend = new MemoryStateBackend();
        DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
        dummyEnv.setKvStateRegistry(registry);
        AbstractKeyedStateBackend<Integer> backend =
                createKeyedStateBackend(registry, numKeyGroups, abstractBackend, dummyEnv);

        final TestRegistryListener registryListener = new TestRegistryListener();
        registry.registerListener(dummyEnv.getJobID(), registryListener);

        // Update the KvState and request it
        int expectedValue = 712828289;

        int key = 99812822;
        backend.setCurrentKey(key);
        ValueState<Integer> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

        state.update(expectedValue);

        byte[] serializedKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        key,
                        IntSerializer.INSTANCE,
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE);

        long requestId = Integer.MAX_VALUE + 182828L;

        assertThat(registryListener.registrationName).isEqualTo("vanilla");

        KvStateInternalRequest request =
                new KvStateInternalRequest(registryListener.kvStateId, serializedKeyAndNamespace);

        ByteBuf serRequest =
                MessageSerializer.serializeRequest(channel.alloc(), requestId, request);

        // Write the request and wait for the response
        channel.writeInbound(serRequest);

        ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_RESULT);
        long deserRequestId = MessageSerializer.getRequestId(buf);
        KvStateResponse response = serializer.deserializeResponse(buf);
        buf.release();

        assertThat(deserRequestId).isEqualTo(requestId);

        int actualValue =
                KvStateSerializer.deserializeValue(response.getContent(), IntSerializer.INSTANCE);
        assertThat(actualValue).isEqualTo(expectedValue);

        assertThat(stats.getNumRequests()).isEqualTo(1).withFailMessage(stats.toString());

        // Wait for async successful request report
        long deadline = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
        while (stats.getNumSuccessful() != 1L && System.nanoTime() <= deadline) {
            Thread.sleep(10L);
        }

        assertThat(stats.getNumSuccessful()).isEqualTo(1L).withFailMessage(stats.toString());
    }

    /**
     * Tests the failure response with {@link UnknownKvStateIdException} as cause on queries for
     * unregistered KvStateIDs.
     */
    @Test
    void testQueryUnknownKvStateID() throws Exception {
        KvStateRegistry registry = new KvStateRegistry();
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(testServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

        long requestId = Integer.MAX_VALUE + 182828L;

        KvStateInternalRequest request = new KvStateInternalRequest(new KvStateID(), new byte[0]);

        ByteBuf serRequest =
                MessageSerializer.serializeRequest(channel.alloc(), requestId, request);

        // Write the request and wait for the response
        channel.writeInbound(serRequest);

        ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_FAILURE);
        RequestFailure response = MessageSerializer.deserializeRequestFailure(buf);
        buf.release();

        assertThat(response.getRequestId()).isEqualTo(requestId);

        assertThat(response.getCause())
                .isInstanceOf(UnknownKvStateIdException.class)
                .withFailMessage("Did not respond with expected failure cause");

        assertThat(stats.getNumRequests()).isEqualTo(1L);
        assertThat(stats.getNumFailed()).isEqualTo(1L);
    }

    /**
     * Tests the failure response with {@link UnknownKeyOrNamespaceException} as cause on queries
     * for non-existing keys.
     */
    @Test
    void testQueryUnknownKey() throws Exception {
        KvStateRegistry registry = new KvStateRegistry();
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(testServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

        int numKeyGroups = 1;
        AbstractStateBackend abstractBackend = new MemoryStateBackend();
        DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
        dummyEnv.setKvStateRegistry(registry);
        KeyedStateBackend<Integer> backend =
                createKeyedStateBackend(registry, numKeyGroups, abstractBackend, dummyEnv);

        final TestRegistryListener registryListener = new TestRegistryListener();
        registry.registerListener(dummyEnv.getJobID(), registryListener);

        // Register state
        ValueStateDescriptor<Integer> desc =
                new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
        desc.setQueryable("vanilla");

        backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

        byte[] serializedKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        1238283,
                        IntSerializer.INSTANCE,
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE);

        long requestId = Integer.MAX_VALUE + 22982L;

        assertThat(registryListener.registrationName).isEqualTo("vanilla");

        KvStateInternalRequest request =
                new KvStateInternalRequest(registryListener.kvStateId, serializedKeyAndNamespace);
        ByteBuf serRequest =
                MessageSerializer.serializeRequest(channel.alloc(), requestId, request);

        // Write the request and wait for the response
        channel.writeInbound(serRequest);

        ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_FAILURE);
        RequestFailure response = MessageSerializer.deserializeRequestFailure(buf);
        buf.release();

        assertThat(response.getRequestId()).isEqualTo(requestId);

        assertThat(response.getCause())
                .isInstanceOf(UnknownKeyOrNamespaceException.class)
                .withFailMessage("Did not respond with expected failure cause");

        assertThat(stats.getNumRequests()).isEqualTo(1L);
        assertThat(stats.getNumFailed()).isEqualTo(1L);
    }

    /**
     * Tests the failure response on a failure on the {@link
     * InternalKvState#getSerializedValue(byte[], TypeSerializer, TypeSerializer, TypeSerializer)}
     * call.
     */
    @Test
    void testFailureOnGetSerializedValue() throws Exception {
        KvStateRegistry registry = new KvStateRegistry();
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(testServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

        // Failing KvState
        InternalKvState<Integer, VoidNamespace, Long> kvState =
                new InternalKvState<Integer, VoidNamespace, Long>() {
                    @Override
                    public TypeSerializer<Integer> getKeySerializer() {
                        return IntSerializer.INSTANCE;
                    }

                    @Override
                    public TypeSerializer<VoidNamespace> getNamespaceSerializer() {
                        return VoidNamespaceSerializer.INSTANCE;
                    }

                    @Override
                    public TypeSerializer<Long> getValueSerializer() {
                        return LongSerializer.INSTANCE;
                    }

                    @Override
                    public void setCurrentNamespace(VoidNamespace namespace) {
                        // do nothing
                    }

                    @Override
                    public byte[] getSerializedValue(
                            final byte[] serializedKeyAndNamespace,
                            final TypeSerializer<Integer> safeKeySerializer,
                            final TypeSerializer<VoidNamespace> safeNamespaceSerializer,
                            final TypeSerializer<Long> safeValueSerializer)
                            throws Exception {
                        throw new RuntimeException("Expected test Exception");
                    }

                    @Override
                    public StateIncrementalVisitor<Integer, VoidNamespace, Long>
                            getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public void clear() {}
                };

        KvStateID kvStateId =
                registry.registerKvState(
                        new JobID(),
                        new JobVertexID(),
                        new KeyGroupRange(0, 0),
                        "vanilla",
                        kvState,
                        getClass().getClassLoader());

        KvStateInternalRequest request = new KvStateInternalRequest(kvStateId, new byte[0]);
        ByteBuf serRequest = MessageSerializer.serializeRequest(channel.alloc(), 282872L, request);

        // Write the request and wait for the response
        channel.writeInbound(serRequest);

        ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_FAILURE);
        RequestFailure response = MessageSerializer.deserializeRequestFailure(buf);
        buf.release();

        assertThat(response.getCause().getMessage()).contains("Expected test Exception");

        assertThat(stats.getNumRequests()).isEqualTo(1L);
        assertThat(stats.getNumFailed()).isEqualTo(1L);
    }

    /** Tests that the channel is closed if an Exception reaches the channel handler. */
    @Test
    void testCloseChannelOnExceptionCaught() throws Exception {
        KvStateRegistry registry = new KvStateRegistry();
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(testServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(handler);

        channel.pipeline().fireExceptionCaught(new RuntimeException("Expected test Exception"));

        ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.SERVER_FAILURE);
        Throwable response = MessageSerializer.deserializeServerFailure(buf);
        buf.release();

        assertThat(response.getMessage()).contains("Expected test Exception");

        channel.closeFuture().await(READ_TIMEOUT_MILLIS);
        assertThat(channel.isActive()).isFalse();
    }

    /**
     * Tests the failure response on a rejected execution, because the query executor has been
     * closed.
     */
    @Test
    void testQueryExecutorShutDown() throws Throwable {
        KvStateRegistry registry = new KvStateRegistry();
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        KvStateServerImpl localTestServer =
                new KvStateServerImpl(
                        InetAddress.getLocalHost().getHostName(),
                        Collections.singletonList(0).iterator(),
                        1,
                        1,
                        new KvStateRegistry(),
                        new DisabledKvStateRequestStats());

        localTestServer.start();
        localTestServer.shutdown();
        assertThat(localTestServer.getQueryExecutor().isTerminated()).isTrue();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(localTestServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

        int numKeyGroups = 1;
        AbstractStateBackend abstractBackend = new MemoryStateBackend();
        DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
        dummyEnv.setKvStateRegistry(registry);
        KeyedStateBackend<Integer> backend =
                createKeyedStateBackend(registry, numKeyGroups, abstractBackend, dummyEnv);

        final TestRegistryListener registryListener = new TestRegistryListener();
        registry.registerListener(dummyEnv.getJobID(), registryListener);

        // Register state
        ValueStateDescriptor<Integer> desc =
                new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
        desc.setQueryable("vanilla");

        backend.getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

        assertThat(registryListener.registrationName).isEqualTo("vanilla");

        KvStateInternalRequest request =
                new KvStateInternalRequest(registryListener.kvStateId, new byte[0]);
        ByteBuf serRequest = MessageSerializer.serializeRequest(channel.alloc(), 282872L, request);

        // Write the request and wait for the response
        channel.writeInbound(serRequest);

        ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_FAILURE);
        RequestFailure response = MessageSerializer.deserializeRequestFailure(buf);
        buf.release();

        assertThat(response.getCause().getMessage()).contains("RejectedExecutionException");

        assertThat(stats.getNumRequests()).isEqualTo(1L);
        assertThat(stats.getNumFailed()).isEqualTo(1L);

        localTestServer.shutdown();
    }

    /** Tests response on unexpected messages. */
    @Test
    void testUnexpectedMessage() throws Exception {
        KvStateRegistry registry = new KvStateRegistry();
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(testServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

        // Write the request and wait for the response
        ByteBuf unexpectedMessage = Unpooled.buffer(8);
        unexpectedMessage.writeInt(4);
        unexpectedMessage.writeInt(123238213);

        channel.writeInbound(unexpectedMessage);

        ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.SERVER_FAILURE);
        Throwable response = MessageSerializer.deserializeServerFailure(buf);
        buf.release();

        assertThat(stats.getNumRequests()).isEqualTo(0L);
        assertThat(stats.getNumFailed()).isEqualTo(0L);

        KvStateResponse stateResponse = new KvStateResponse(new byte[0]);
        unexpectedMessage =
                MessageSerializer.serializeResponse(channel.alloc(), 192L, stateResponse);

        channel.writeInbound(unexpectedMessage);

        buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.SERVER_FAILURE);
        response = MessageSerializer.deserializeServerFailure(buf);
        buf.release();

        assertThat(response)
                .isInstanceOf(IllegalArgumentException.class)
                .withFailMessage("Unexpected failure cause " + response.getClass().getName());

        assertThat(stats.getNumRequests()).isEqualTo(0L);
        assertThat(stats.getNumFailed()).isEqualTo(0L);
    }

    /** Tests that incoming buffer instances are recycled. */
    @Test
    void testIncomingBufferIsRecycled() throws Exception {
        KvStateRegistry registry = new KvStateRegistry();
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(testServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

        KvStateInternalRequest request = new KvStateInternalRequest(new KvStateID(), new byte[0]);
        ByteBuf serRequest = MessageSerializer.serializeRequest(channel.alloc(), 282872L, request);

        assertThat(serRequest.refCnt()).isEqualTo(1L);

        // Write regular request
        channel.writeInbound(serRequest);
        assertThat(serRequest.refCnt()).isEqualTo(0L).withFailMessage("Buffer not recycled");

        // Write unexpected msg
        ByteBuf unexpected = channel.alloc().buffer(8);
        unexpected.writeInt(4);
        unexpected.writeInt(4);

        assertThat(unexpected.refCnt()).isEqualTo(1L);

        channel.writeInbound(unexpected);
        assertThat(unexpected.refCnt()).isEqualTo(0L).withFailMessage("Buffer not recycled");
        channel.finishAndReleaseAll();
    }

    /** Tests the failure response if the serializers don't match. */
    @Test
    void testSerializerMismatch() throws Exception {
        KvStateRegistry registry = new KvStateRegistry();
        AtomicKvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(testServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

        int numKeyGroups = 1;
        AbstractStateBackend abstractBackend = new MemoryStateBackend();
        DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
        dummyEnv.setKvStateRegistry(registry);
        AbstractKeyedStateBackend<Integer> backend =
                createKeyedStateBackend(registry, numKeyGroups, abstractBackend, dummyEnv);

        final TestRegistryListener registryListener = new TestRegistryListener();
        registry.registerListener(dummyEnv.getJobID(), registryListener);

        // Register state
        ValueStateDescriptor<Integer> desc =
                new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
        desc.setQueryable("vanilla");

        ValueState<Integer> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

        int key = 99812822;

        // Update the KvState
        backend.setCurrentKey(key);
        state.update(712828289);

        byte[] wrongKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        "wrong-key-type",
                        StringSerializer.INSTANCE,
                        "wrong-namespace-type",
                        StringSerializer.INSTANCE);

        byte[] wrongNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        key,
                        IntSerializer.INSTANCE,
                        "wrong-namespace-type",
                        StringSerializer.INSTANCE);

        assertThat(registryListener.registrationName).isEqualTo("vanilla");

        KvStateInternalRequest request =
                new KvStateInternalRequest(registryListener.kvStateId, wrongKeyAndNamespace);
        ByteBuf serRequest = MessageSerializer.serializeRequest(channel.alloc(), 182828L, request);

        // Write the request and wait for the response
        channel.writeInbound(serRequest);

        ByteBuf buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_FAILURE);
        RequestFailure response = MessageSerializer.deserializeRequestFailure(buf);
        buf.release();
        assertThat(response.getRequestId()).isEqualTo(182828L);
        assertThat(response.getCause().getMessage()).contains("IOException");

        // Repeat with wrong namespace only
        request = new KvStateInternalRequest(registryListener.kvStateId, wrongNamespace);
        serRequest = MessageSerializer.serializeRequest(channel.alloc(), 182829L, request);

        // Write the request and wait for the response
        channel.writeInbound(serRequest);

        buf = (ByteBuf) readInboundBlocking(channel);
        buf.skipBytes(4); // skip frame length

        // Verify the response
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_FAILURE);
        response = MessageSerializer.deserializeRequestFailure(buf);
        buf.release();
        assertThat(response.getRequestId()).isEqualTo(182829L);
        assertThat(response.getCause().getMessage()).contains("IOException");

        assertThat(stats.getNumRequests()).isEqualTo(2L);
        assertThat(stats.getNumFailed()).isEqualTo(2L);
    }

    /** Tests that large responses are chunked. */
    @Test
    void testChunkedResponse() throws Exception {
        KvStateRegistry registry = new KvStateRegistry();
        KvStateRequestStats stats = new AtomicKvStateRequestStats();

        MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        KvStateServerHandler handler =
                new KvStateServerHandler(testServer, registry, serializer, stats);
        EmbeddedChannel channel = new EmbeddedChannel(getFrameDecoder(), handler);

        int numKeyGroups = 1;
        AbstractStateBackend abstractBackend = new MemoryStateBackend();
        DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
        dummyEnv.setKvStateRegistry(registry);
        AbstractKeyedStateBackend<Integer> backend =
                createKeyedStateBackend(registry, numKeyGroups, abstractBackend, dummyEnv);

        final TestRegistryListener registryListener = new TestRegistryListener();
        registry.registerListener(dummyEnv.getJobID(), registryListener);

        // Register state
        ValueStateDescriptor<byte[]> desc =
                new ValueStateDescriptor<>("any", BytePrimitiveArraySerializer.INSTANCE);
        desc.setQueryable("vanilla");

        ValueState<byte[]> state =
                backend.getPartitionedState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, desc);

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
        byte[] serializedKeyAndNamespace =
                KvStateSerializer.serializeKeyAndNamespace(
                        key,
                        IntSerializer.INSTANCE,
                        VoidNamespace.INSTANCE,
                        VoidNamespaceSerializer.INSTANCE);

        long requestId = Integer.MAX_VALUE + 182828L;

        assertThat(registryListener.registrationName).isEqualTo("vanilla");
        KvStateInternalRequest request =
                new KvStateInternalRequest(registryListener.kvStateId, serializedKeyAndNamespace);
        ByteBuf serRequest =
                MessageSerializer.serializeRequest(channel.alloc(), requestId, request);

        // Write the request and wait for the response
        channel.writeInbound(serRequest);

        Object msg = readInboundBlocking(channel);
        assertThat(msg).isInstanceOf(ChunkedByteBuf.class).withFailMessage("Not ChunkedByteBuf");
        ((ChunkedByteBuf) msg).close();
    }

    // ------------------------------------------------------------------------

    /** Queries the embedded channel for data. */
    private Object readInboundBlocking(EmbeddedChannel channel)
            throws InterruptedException, TimeoutException {
        final long sleepMillis = 50L;

        long sleptMillis = 0L;

        Object msg = null;
        while (sleptMillis < READ_TIMEOUT_MILLIS && (msg = channel.readOutbound()) == null) {

            Thread.sleep(sleepMillis);
            sleptMillis += sleepMillis;
        }

        if (msg == null) {
            throw new TimeoutException();
        } else {
            return msg;
        }
    }

    /** Frame length decoder (expected by the serialized messages). */
    private ChannelHandler getFrameDecoder() {
        return new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4);
    }

    /**
     * A listener that keeps the last updated KvState information so that a test can retrieve it.
     */
    static class TestRegistryListener implements KvStateRegistryListener {
        volatile JobVertexID jobVertexID;
        volatile KeyGroupRange keyGroupIndex;
        volatile String registrationName;
        volatile KvStateID kvStateId;

        @Override
        public void notifyKvStateRegistered(
                JobID jobId,
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
        public void notifyKvStateUnregistered(
                JobID jobId,
                JobVertexID jobVertexId,
                KeyGroupRange keyGroupRange,
                String registrationName) {}
    }

    private AbstractKeyedStateBackend<Integer> createKeyedStateBackend(
            KvStateRegistry registry,
            int numKeyGroups,
            AbstractStateBackend abstractBackend,
            DummyEnvironment dummyEnv)
            throws java.io.IOException {
        return abstractBackend.createKeyedStateBackend(
                dummyEnv,
                dummyEnv.getJobID(),
                "test_op",
                IntSerializer.INSTANCE,
                numKeyGroups,
                new KeyGroupRange(0, 0),
                registry.createTaskRegistry(dummyEnv.getJobID(), dummyEnv.getJobVertexId()),
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                Collections.emptyList(),
                new CloseableRegistry());
    }
}
