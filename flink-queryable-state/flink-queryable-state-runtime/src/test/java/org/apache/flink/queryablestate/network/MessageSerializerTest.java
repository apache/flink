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

import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.messages.KvStateInternalRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.messages.MessageType;
import org.apache.flink.queryablestate.network.messages.RequestFailure;

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

/** Tests for {@link MessageSerializer}. */
@RunWith(Parameterized.class)
public class MessageSerializerTest {

    private final ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;

    @Parameterized.Parameters
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameterized.Parameter public boolean async;

    /** Tests request serialization. */
    @Test
    public void testRequestSerialization() throws Exception {
        long requestId = Integer.MAX_VALUE + 1337L;
        KvStateID kvStateId = new KvStateID();
        byte[] serializedKeyAndNamespace = randomByteArray(1024);

        final KvStateInternalRequest request =
                new KvStateInternalRequest(kvStateId, serializedKeyAndNamespace);
        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        ByteBuf buf = MessageSerializer.serializeRequest(alloc, requestId, request);

        int frameLength = buf.readInt();
        assertEquals(MessageType.REQUEST, MessageSerializer.deserializeHeader(buf));
        assertEquals(requestId, MessageSerializer.getRequestId(buf));
        KvStateInternalRequest requestDeser = serializer.deserializeRequest(buf);

        assertEquals(buf.readerIndex(), frameLength + 4);

        assertEquals(kvStateId, requestDeser.getKvStateId());
        assertArrayEquals(serializedKeyAndNamespace, requestDeser.getSerializedKeyAndNamespace());
    }

    /** Tests request serialization with zero-length serialized key and namespace. */
    @Test
    public void testRequestSerializationWithZeroLengthKeyAndNamespace() throws Exception {

        long requestId = Integer.MAX_VALUE + 1337L;
        KvStateID kvStateId = new KvStateID();
        byte[] serializedKeyAndNamespace = new byte[0];

        final KvStateInternalRequest request =
                new KvStateInternalRequest(kvStateId, serializedKeyAndNamespace);
        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        ByteBuf buf = MessageSerializer.serializeRequest(alloc, requestId, request);

        int frameLength = buf.readInt();
        assertEquals(MessageType.REQUEST, MessageSerializer.deserializeHeader(buf));
        assertEquals(requestId, MessageSerializer.getRequestId(buf));
        KvStateInternalRequest requestDeser = serializer.deserializeRequest(buf);

        assertEquals(buf.readerIndex(), frameLength + 4);

        assertEquals(kvStateId, requestDeser.getKvStateId());
        assertArrayEquals(serializedKeyAndNamespace, requestDeser.getSerializedKeyAndNamespace());
    }

    /**
     * Tests that we don't try to be smart about <code>null</code> key and namespace. They should be
     * treated explicitly.
     */
    @Test(expected = NullPointerException.class)
    public void testNullPointerExceptionOnNullSerializedKeyAndNamepsace() throws Exception {
        new KvStateInternalRequest(new KvStateID(), null);
    }

    /** Tests response serialization. */
    @Test
    public void testResponseSerialization() throws Exception {
        long requestId = Integer.MAX_VALUE + 72727278L;
        byte[] serializedResult = randomByteArray(1024);

        final KvStateResponse response = new KvStateResponse(serializedResult);
        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        ByteBuf buf = MessageSerializer.serializeResponse(alloc, requestId, response);

        int frameLength = buf.readInt();
        assertEquals(MessageType.REQUEST_RESULT, MessageSerializer.deserializeHeader(buf));
        assertEquals(requestId, MessageSerializer.getRequestId(buf));
        KvStateResponse responseDeser = serializer.deserializeResponse(buf);

        assertEquals(buf.readerIndex(), frameLength + 4);

        assertArrayEquals(serializedResult, responseDeser.getContent());
    }

    /** Tests response serialization with zero-length serialized result. */
    @Test
    public void testResponseSerializationWithZeroLengthSerializedResult() throws Exception {
        byte[] serializedResult = new byte[0];

        final KvStateResponse response = new KvStateResponse(serializedResult);
        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        ByteBuf buf = MessageSerializer.serializeResponse(alloc, 72727278L, response);

        int frameLength = buf.readInt();

        assertEquals(MessageType.REQUEST_RESULT, MessageSerializer.deserializeHeader(buf));
        assertEquals(72727278L, MessageSerializer.getRequestId(buf));
        KvStateResponse responseDeser = serializer.deserializeResponse(buf);
        assertEquals(buf.readerIndex(), frameLength + 4);

        assertArrayEquals(serializedResult, responseDeser.getContent());
    }

    /**
     * Tests that we don't try to be smart about <code>null</code> results. They should be treated
     * explicitly.
     */
    @Test(expected = NullPointerException.class)
    public void testNullPointerExceptionOnNullSerializedResult() throws Exception {
        new KvStateResponse((byte[]) null);
    }

    /** Tests request failure serialization. */
    @Test
    public void testKvStateRequestFailureSerialization() throws Exception {
        long requestId = Integer.MAX_VALUE + 1111222L;
        IllegalStateException cause = new IllegalStateException("Expected test");

        ByteBuf buf = MessageSerializer.serializeRequestFailure(alloc, requestId, cause);

        int frameLength = buf.readInt();
        assertEquals(MessageType.REQUEST_FAILURE, MessageSerializer.deserializeHeader(buf));
        RequestFailure requestFailure = MessageSerializer.deserializeRequestFailure(buf);
        assertEquals(buf.readerIndex(), frameLength + 4);

        assertEquals(requestId, requestFailure.getRequestId());
        assertEquals(cause.getClass(), requestFailure.getCause().getClass());
        assertEquals(cause.getMessage(), requestFailure.getCause().getMessage());
    }

    /** Tests server failure serialization. */
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
