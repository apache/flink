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

import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MessageSerializer}. */
class MessageSerializerTest {

    private final ByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;

    /** Tests request serialization. */
    @Test
    void testRequestSerialization() throws Exception {
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
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST);
        assertThat(MessageSerializer.getRequestId(buf)).isEqualTo(requestId);
        KvStateInternalRequest requestDeser = serializer.deserializeRequest(buf);

        assertThat(buf.readerIndex()).isEqualTo(frameLength + 4);

        assertThat(requestDeser.getKvStateId()).isEqualTo(kvStateId);
        assertThat(requestDeser.getSerializedKeyAndNamespace())
                .isEqualTo(serializedKeyAndNamespace);
    }

    /** Tests request serialization with zero-length serialized key and namespace. */
    @Test
    void testRequestSerializationWithZeroLengthKeyAndNamespace() throws Exception {

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
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST);
        assertThat(MessageSerializer.getRequestId(buf)).isEqualTo(requestId);
        KvStateInternalRequest requestDeser = serializer.deserializeRequest(buf);

        assertThat(buf.readerIndex()).isEqualTo(frameLength + 4);

        assertThat(requestDeser.getKvStateId()).isEqualTo(kvStateId);
        assertThat(requestDeser.getSerializedKeyAndNamespace())
                .isEqualTo(serializedKeyAndNamespace);
    }

    /**
     * Tests that we don't try to be smart about <code>null</code> key and namespace. They should be
     * treated explicitly.
     */
    @Test
    void testNullPointerExceptionOnNullSerializedKeyAndNamepsace() throws Exception {
        assertThatThrownBy(() -> new KvStateInternalRequest(new KvStateID(), null))
                .isInstanceOf(NullPointerException.class);
    }

    /** Tests response serialization. */
    @Test
    void testResponseSerialization() throws Exception {
        long requestId = Integer.MAX_VALUE + 72727278L;
        byte[] serializedResult = randomByteArray(1024);

        final KvStateResponse response = new KvStateResponse(serializedResult);
        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        ByteBuf buf = MessageSerializer.serializeResponse(alloc, requestId, response);

        int frameLength = buf.readInt();
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_RESULT);
        assertThat(MessageSerializer.getRequestId(buf)).isEqualTo(requestId);
        KvStateResponse responseDeser = serializer.deserializeResponse(buf);

        assertThat(buf.readerIndex()).isEqualTo(frameLength + 4);

        assertThat(responseDeser.getContent()).isEqualTo(serializedResult);
    }

    /** Tests response serialization with zero-length serialized result. */
    @Test
    void testResponseSerializationWithZeroLengthSerializedResult() throws Exception {
        byte[] serializedResult = new byte[0];

        final KvStateResponse response = new KvStateResponse(serializedResult);
        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());

        ByteBuf buf = MessageSerializer.serializeResponse(alloc, 72727278L, response);

        int frameLength = buf.readInt();

        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_RESULT);
        assertThat(MessageSerializer.getRequestId(buf)).isEqualTo(72727278L);
        KvStateResponse responseDeser = serializer.deserializeResponse(buf);
        assertThat(buf.readerIndex()).isEqualTo(frameLength + 4);

        assertThat(responseDeser.getContent()).isEqualTo(serializedResult);
    }

    /**
     * Tests that we don't try to be smart about <code>null</code> results. They should be treated
     * explicitly.
     */
    @Test
    void testNullPointerExceptionOnNullSerializedResult() throws Exception {
        assertThatThrownBy(() -> new KvStateResponse((byte[]) null))
                .isInstanceOf(NullPointerException.class);
    }

    /** Tests request failure serialization. */
    @Test
    void testKvStateRequestFailureSerialization() throws Exception {
        long requestId = Integer.MAX_VALUE + 1111222L;
        IllegalStateException cause = new IllegalStateException("Expected test");

        ByteBuf buf = MessageSerializer.serializeRequestFailure(alloc, requestId, cause);

        int frameLength = buf.readInt();
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.REQUEST_FAILURE);
        RequestFailure requestFailure = MessageSerializer.deserializeRequestFailure(buf);
        assertThat(buf.readerIndex()).isEqualTo(frameLength + 4);

        assertThat(requestFailure.getRequestId()).isEqualTo(requestId);
        assertThat(requestFailure.getCause()).isInstanceOf(cause.getClass());
        assertThat(requestFailure.getCause().getMessage()).isEqualTo(cause.getMessage());
    }

    /** Tests server failure serialization. */
    @Test
    void testServerFailureSerialization() throws Exception {
        IllegalStateException cause = new IllegalStateException("Expected test");

        ByteBuf buf = MessageSerializer.serializeServerFailure(alloc, cause);

        int frameLength = buf.readInt();
        assertThat(MessageSerializer.deserializeHeader(buf)).isEqualTo(MessageType.SERVER_FAILURE);
        Throwable request = MessageSerializer.deserializeServerFailure(buf);
        assertThat(buf.readerIndex()).isEqualTo(frameLength + 4);

        assertThat(request).isInstanceOf(cause.getClass());
        assertThat(request.getMessage()).isEqualTo(cause.getMessage());
    }

    private byte[] randomByteArray(int capacity) {
        byte[] bytes = new byte[capacity];
        ThreadLocalRandom.current().nextBytes(bytes);
        return bytes;
    }
}
