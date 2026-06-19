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

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse.MESSAGE_HEADER_LENGTH;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The decoder for {@link BufferResponse}. */
class BufferResponseDecoder extends NettyMessageDecoder {

    /** The Buffer allocator. */
    private final NetworkBufferAllocator allocator;

    /** The accumulation buffer of message header. */
    private ByteBuf messageHeaderBuffer;

    /**
     * Accumulates bytes for a partial buffer size.
     *
     * <p>In scenarios such as Sort-merge shuffle, where small buffers are merged, each small buffer
     * size is written sequentially in the data stream. This list temporarily holds the bytes needed
     * to form a full integer representation of a buffer's size when there aren't sufficient bytes
     * to read an integer directly from the incoming data buffer.
     */
    private List<Byte> partialSizeBytes;

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

            decodePartialBufferSizes(data);

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

    /**
     * Decodes the sizes of partial buffers from the provided ByteBuf. This function processes the
     * incoming data and accumulates bytes until a full integer can be formed to represent the size
     * of each buffer.
     *
     * @param data the ByteBuf containing the incoming data.
     */
    private void decodePartialBufferSizes(ByteBuf data) {
        // If partial buffers are present and not all are processed yet
        if (bufferResponse.numOfPartialBuffers > 0
                && bufferResponse.getPartialBufferSizes().size()
                        < bufferResponse.numOfPartialBuffers) {

            // Continue completing the current partial buffer size if necessary
            accumulatePartialSizeBytes(data);

            // Process remaining partial buffer sizes when possible
            readRemainingBufferSizes(data);
        }
    }

    /**
     * Accumulates bytes to form a complete integer size for a partial buffer. If enough bytes are
     * accumulated, forms an integer and adds it to bufferResponse list.
     *
     * @param data the ByteBuf containing the incoming data.
     */
    private void accumulatePartialSizeBytes(ByteBuf data) {
        if (partialSizeBytes != null) {
            while (partialSizeBytes.size() < Integer.BYTES && data.isReadable()) {
                partialSizeBytes.add(data.readByte());
            }
            if (partialSizeBytes.size() == Integer.BYTES) {
                int size = buildIntFromBytes(partialSizeBytes);
                bufferResponse.getPartialBufferSizes().add(size);
                partialSizeBytes = null;
            }
        }
    }

    /**
     * Reads remaining complete partial buffer sizes directly from the ByteBuf if possible. Prepares
     * for partially available sizes by initializing byte accumulator.
     *
     * @param data the ByteBuf containing the incoming data.
     */
    private void readRemainingBufferSizes(ByteBuf data) {
        while (data.isReadable()
                && bufferResponse.getPartialBufferSizes().size()
                        < bufferResponse.numOfPartialBuffers) {
            if (data.readableBytes() >= Integer.BYTES) {
                bufferResponse.getPartialBufferSizes().add(data.readInt());
            } else {
                partialSizeBytes = new ArrayList<>();
                while (data.isReadable()) {
                    partialSizeBytes.add(data.readByte());
                }
            }
        }
    }

    /**
     * Converts a list of four bytes into an integer.
     *
     * @param byteList the list containing four bytes.
     * @return the constructed integer.
     */
    private int buildIntFromBytes(List<Byte> byteList) {
        checkState(byteList.size() == Integer.BYTES);
        return ((byteList.get(0) & 0xFF) << 24)
                | ((byteList.get(1) & 0xFF) << 16)
                | ((byteList.get(2) & 0xFF) << 8)
                | (byteList.get(3) & 0xFF);
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
