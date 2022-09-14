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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import java.net.ProtocolException;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BacklogAnnouncement;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;

/** The decoder for messages other than {@link BufferResponse}. */
class NonBufferResponseDecoder extends NettyMessageDecoder {

    /** The initial size of the message header accumulation buffer. */
    private static final int INITIAL_MESSAGE_HEADER_BUFFER_LENGTH = 128;

    /** The accumulation buffer of the message. */
    private ByteBuf messageBuffer;

    @Override
    public void onChannelActive(ChannelHandlerContext ctx) {
        messageBuffer = ctx.alloc().directBuffer(INITIAL_MESSAGE_HEADER_BUFFER_LENGTH);
    }

    @Override
    void onNewMessageReceived(int msgId, int messageLength) {
        super.onNewMessageReceived(msgId, messageLength);
        messageBuffer.clear();
        ensureBufferCapacity();
    }

    @Override
    public DecodingResult onChannelRead(ByteBuf data) throws Exception {
        ByteBuf fullFrameHeaderBuf =
                ByteBufUtils.accumulate(
                        messageBuffer, data, messageLength, messageBuffer.readableBytes());
        if (fullFrameHeaderBuf == null) {
            return DecodingResult.NOT_FINISHED;
        }

        switch (msgId) {
            case ErrorResponse.ID:
                return DecodingResult.fullMessage(ErrorResponse.readFrom(fullFrameHeaderBuf));
            case BacklogAnnouncement.ID:
                return DecodingResult.fullMessage(BacklogAnnouncement.readFrom(fullFrameHeaderBuf));
            default:
                throw new ProtocolException("Received unknown message from producer: " + msgId);
        }
    }

    /**
     * Ensures the message header accumulation buffer has enough capacity for the current message.
     */
    private void ensureBufferCapacity() {
        if (messageBuffer.capacity() < messageLength) {
            messageBuffer.capacity(messageLength);
        }
    }

    @Override
    public void close() {
        messageBuffer.release();
    }
}
