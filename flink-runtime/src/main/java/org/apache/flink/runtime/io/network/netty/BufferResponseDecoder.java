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

import javax.annotation.Nullable;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse.MESSAGE_HEADER_LENGTH;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The decoder for {@link BufferResponse}. */
class BufferResponseDecoder extends NettyMessageDecoder {

    /** The Buffer allocator. */
    private final NetworkBufferAllocator allocator;

    /** The accumulation buffer of message header. */
    private ByteBuf messageHeaderBuffer;

    /**
     * The BufferResponse message that has its message header decoded, but still not received all
     * the bytes of the buffer part.
     */
    @Nullable private BufferResponse bufferResponse;

    /** How many bytes have been received or discarded for the data buffer part. */
    private int decodedDataBufferSize;

    BufferResponseDecoder(NetworkBufferAllocator allocator) {
        this.allocator = checkNotNull(allocator);
    }

    @Override
    public void onChannelActive(ChannelHandlerContext ctx) {
        messageHeaderBuffer = ctx.alloc().directBuffer(MESSAGE_HEADER_LENGTH);
    }

    @Override
    public DecodingResult onChannelRead(ByteBuf data) throws Exception {
        if (bufferResponse == null) {
            decodeMessageHeader(data);
        }

        if (bufferResponse != null) {
            int remainingBufferSize = bufferResponse.bufferSize - decodedDataBufferSize;
            int actualBytesToDecode = Math.min(data.readableBytes(), remainingBufferSize);

            // For the case of data buffer really exists in BufferResponse now.
            if (actualBytesToDecode > 0) {
                // For the case of released input channel, the respective data buffer part would be
                // discarded from the received buffer.
                if (bufferResponse.getBuffer() == null) {
                    data.readerIndex(data.readerIndex() + actualBytesToDecode);
                } else {
                    bufferResponse.getBuffer().asByteBuf().writeBytes(data, actualBytesToDecode);
                }

                decodedDataBufferSize += actualBytesToDecode;
            }

            if (decodedDataBufferSize == bufferResponse.bufferSize) {
                BufferResponse result = bufferResponse;
                clearState();
                return DecodingResult.fullMessage(result);
            }
        }

        return DecodingResult.NOT_FINISHED;
    }

    private void decodeMessageHeader(ByteBuf data) {
        ByteBuf fullFrameHeaderBuf =
                ByteBufUtils.accumulate(
                        messageHeaderBuffer,
                        data,
                        MESSAGE_HEADER_LENGTH,
                        messageHeaderBuffer.readableBytes());
        if (fullFrameHeaderBuf != null) {
            bufferResponse = BufferResponse.readFrom(fullFrameHeaderBuf, allocator);
        }
    }

    private void clearState() {
        bufferResponse = null;
        decodedDataBufferSize = 0;

        messageHeaderBuffer.clear();
    }

    @Override
    public void close() {
        if (bufferResponse != null) {
            bufferResponse.releaseBuffer();
        }

        messageHeaderBuffer.release();
    }
}
