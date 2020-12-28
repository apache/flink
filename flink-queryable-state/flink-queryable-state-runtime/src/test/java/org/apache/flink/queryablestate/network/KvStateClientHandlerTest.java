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

import org.apache.flink.queryablestate.messages.KvStateInternalRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import java.nio.channels.ClosedChannelException;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for {@link ClientHandler}. */
public class KvStateClientHandlerTest {

    /**
     * Tests that on reads the expected callback methods are called and read buffers are recycled.
     */
    @Test
    public void testReadCallbacksAndBufferRecycling() throws Exception {
        final ClientHandlerCallback<KvStateResponse> callback = mock(ClientHandlerCallback.class);

        final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer =
                new MessageSerializer<>(
                        new KvStateInternalRequest.KvStateInternalRequestDeserializer(),
                        new KvStateResponse.KvStateResponseDeserializer());
        final EmbeddedChannel channel =
                new EmbeddedChannel(new ClientHandler<>("Test Client", serializer, callback));

        final byte[] content = new byte[0];
        final KvStateResponse response = new KvStateResponse(content);

        //
        // Request success
        //
        ByteBuf buf = MessageSerializer.serializeResponse(channel.alloc(), 1222112277L, response);
        buf.skipBytes(4); // skip frame length

        // Verify callback
        channel.writeInbound(buf);
        verify(callback, times(1)).onRequestResult(eq(1222112277L), any(KvStateResponse.class));
        assertEquals("Buffer not recycled", 0, buf.refCnt());

        //
        // Request failure
        //
        buf =
                MessageSerializer.serializeRequestFailure(
                        channel.alloc(),
                        1222112278,
                        new RuntimeException("Expected test Exception"));
        buf.skipBytes(4); // skip frame length

        // Verify callback
        channel.writeInbound(buf);
        verify(callback, times(1)).onRequestFailure(eq(1222112278L), isA(RuntimeException.class));
        assertEquals("Buffer not recycled", 0, buf.refCnt());

        //
        // Server failure
        //
        buf =
                MessageSerializer.serializeServerFailure(
                        channel.alloc(), new RuntimeException("Expected test Exception"));
        buf.skipBytes(4); // skip frame length

        // Verify callback
        channel.writeInbound(buf);
        verify(callback, times(1)).onFailure(isA(RuntimeException.class));

        //
        // Unexpected messages
        //
        buf = channel.alloc().buffer(4).writeInt(1223823);

        // Verify callback
        channel.writeInbound(buf);
        verify(callback, times(1)).onFailure(isA(IllegalStateException.class));
        assertEquals("Buffer not recycled", 0, buf.refCnt());

        //
        // Exception caught
        //
        channel.pipeline().fireExceptionCaught(new RuntimeException("Expected test Exception"));
        verify(callback, times(3)).onFailure(isA(RuntimeException.class));

        //
        // Channel inactive
        //
        channel.pipeline().fireChannelInactive();
        verify(callback, times(1)).onFailure(isA(ClosedChannelException.class));
    }
}
