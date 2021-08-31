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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.TestingPartitionRequestClient;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyBufferResponseHeader;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.junit.Assert.assertNull;

/** Tests the client side message decoder. */
public class NettyMessageClientDecoderDelegateTest extends TestLogger {

    private static final int BUFFER_SIZE = 1024;

    private static final int NUMBER_OF_BUFFER_RESPONSES = 5;

    private static final NettyBufferPool ALLOCATOR = new NettyBufferPool(1);

    private EmbeddedChannel channel;

    private NetworkBufferPool networkBufferPool;

    private SingleInputGate inputGate;

    private InputChannelID inputChannelId;

    private InputChannelID releasedInputChannelId;

    @Before
    public void setup() throws IOException, InterruptedException {
        CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        networkBufferPool = new NetworkBufferPool(NUMBER_OF_BUFFER_RESPONSES, BUFFER_SIZE);
        channel = new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));

        inputGate = createSingleInputGate(1, networkBufferPool);
        RemoteInputChannel inputChannel =
                createRemoteInputChannel(
                        inputGate, new TestingPartitionRequestClient(), NUMBER_OF_BUFFER_RESPONSES);
        inputGate.setInputChannels(inputChannel);
        inputGate.setup();
        inputChannel.requestSubpartition(0);
        handler.addInputChannel(inputChannel);
        inputChannelId = inputChannel.getInputChannelId();

        SingleInputGate releasedInputGate = createSingleInputGate(1, networkBufferPool);
        RemoteInputChannel releasedInputChannel =
                new InputChannelBuilder().buildRemoteChannel(inputGate);
        releasedInputGate.close();
        handler.addInputChannel(releasedInputChannel);
        releasedInputChannelId = releasedInputChannel.getInputChannelId();
    }

    @After
    public void tearDown() throws IOException {
        if (inputGate != null) {
            inputGate.close();
        }

        if (networkBufferPool != null) {
            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }

        if (channel != null) {
            channel.close();
        }
    }

    /** Verifies that the client side decoder works well for unreleased input channels. */
    @Test
    public void testClientMessageDecode() throws Exception {
        testNettyMessageClientDecoding(false, false, false);
    }

    /**
     * Verifies that the client side decoder works well for empty buffers. Empty buffers should not
     * consume data buffers of the input channels.
     */
    @Test
    public void testClientMessageDecodeWithEmptyBuffers() throws Exception {
        testNettyMessageClientDecoding(true, false, false);
    }

    /**
     * Verifies that the client side decoder works well with buffers sent to a released input
     * channel. The data buffer part should be discarded before reading the next message.
     */
    @Test
    public void testClientMessageDecodeWithReleasedInputChannel() throws Exception {
        testNettyMessageClientDecoding(false, true, false);
    }

    /**
     * Verifies that the client side decoder works well with buffers sent to a removed input
     * channel. The data buffer part should be discarded before reading the next message.
     */
    @Test
    public void testClientMessageDecodeWithRemovedInputChannel() throws Exception {
        testNettyMessageClientDecoding(false, false, true);
    }

    // ------------------------------------------------------------------------------------------------------------------

    private void testNettyMessageClientDecoding(
            boolean hasEmptyBuffer,
            boolean hasBufferForReleasedChannel,
            boolean hasBufferForRemovedChannel)
            throws Exception {

        ByteBuf[] encodedMessages = null;
        List<NettyMessage> decodedMessages = null;
        try {
            List<BufferResponse> messages =
                    createMessageList(
                            hasEmptyBuffer,
                            hasBufferForReleasedChannel,
                            hasBufferForRemovedChannel);

            encodedMessages = encodeMessages(messages);

            List<ByteBuf> partitionedBuffers = repartitionMessages(encodedMessages);
            decodedMessages = decodeMessages(channel, partitionedBuffers);
            verifyDecodedMessages(messages, decodedMessages);
        } finally {
            releaseBuffers(encodedMessages);

            if (decodedMessages != null) {
                for (NettyMessage nettyMessage : decodedMessages) {
                    if (nettyMessage instanceof BufferResponse) {
                        ((BufferResponse) nettyMessage).releaseBuffer();
                    }
                }
            }
        }
    }

    private List<BufferResponse> createMessageList(
            boolean hasEmptyBuffer,
            boolean hasBufferForRemovedChannel,
            boolean hasBufferForReleasedChannel) {

        int seqNumber = 1;
        List<BufferResponse> messages = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_BUFFER_RESPONSES - 1; i++) {
            addBufferResponse(
                    messages,
                    inputChannelId,
                    Buffer.DataType.DATA_BUFFER,
                    BUFFER_SIZE,
                    seqNumber++);
        }

        if (hasEmptyBuffer) {
            addBufferResponse(
                    messages, inputChannelId, Buffer.DataType.DATA_BUFFER, 0, seqNumber++);
        }
        if (hasBufferForReleasedChannel) {
            addBufferResponse(
                    messages,
                    releasedInputChannelId,
                    Buffer.DataType.DATA_BUFFER,
                    BUFFER_SIZE,
                    seqNumber++);
        }
        if (hasBufferForRemovedChannel) {
            addBufferResponse(
                    messages,
                    new InputChannelID(),
                    Buffer.DataType.DATA_BUFFER,
                    BUFFER_SIZE,
                    seqNumber++);
        }

        addBufferResponse(messages, inputChannelId, Buffer.DataType.EVENT_BUFFER, 32, seqNumber++);
        addBufferResponse(
                messages, inputChannelId, Buffer.DataType.DATA_BUFFER, BUFFER_SIZE, seqNumber);

        return messages;
    }

    private void addBufferResponse(
            List<BufferResponse> messages,
            InputChannelID inputChannelId,
            Buffer.DataType dataType,
            int bufferSize,
            int seqNumber) {

        Buffer buffer = createDataBuffer(bufferSize, dataType);
        messages.add(new BufferResponse(buffer, seqNumber, inputChannelId, 1));
    }

    private Buffer createDataBuffer(int size, Buffer.DataType dataType) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(size);
        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType);
        for (int i = 0; i < size / 4; ++i) {
            buffer.writeInt(i);
        }

        return buffer;
    }

    private ByteBuf[] encodeMessages(List<BufferResponse> messages) throws Exception {
        ByteBuf[] encodedMessages = new ByteBuf[messages.size()];
        for (int i = 0; i < messages.size(); ++i) {
            encodedMessages[i] = messages.get(i).write(ALLOCATOR);
        }

        return encodedMessages;
    }

    private List<ByteBuf> repartitionMessages(ByteBuf[] encodedMessages) {
        List<ByteBuf> result = new ArrayList<>();

        ByteBuf mergedBuffer1 = null;
        ByteBuf mergedBuffer2 = null;

        try {
            mergedBuffer1 = mergeBuffers(encodedMessages, 0, encodedMessages.length / 2);
            mergedBuffer2 =
                    mergeBuffers(
                            encodedMessages, encodedMessages.length / 2, encodedMessages.length);

            result.addAll(partitionBuffer(mergedBuffer1, BUFFER_SIZE * 2));
            result.addAll(partitionBuffer(mergedBuffer2, BUFFER_SIZE / 4));
        } catch (Throwable t) {
            releaseBuffers(result.toArray(new ByteBuf[0]));
            ExceptionUtils.rethrow(t);
        } finally {
            releaseBuffers(mergedBuffer1, mergedBuffer2);
        }

        return result;
    }

    private ByteBuf mergeBuffers(ByteBuf[] buffers, int start, int end) {
        ByteBuf mergedBuffer = ALLOCATOR.buffer();
        for (int i = start; i < end; ++i) {
            mergedBuffer.writeBytes(buffers[i]);
        }

        return mergedBuffer;
    }

    private List<ByteBuf> partitionBuffer(ByteBuf buffer, int partitionSize) {
        List<ByteBuf> result = new ArrayList<>();

        try {
            int bufferSize = buffer.readableBytes();
            for (int position = 0; position < bufferSize; position += partitionSize) {
                int endPosition = Math.min(position + partitionSize, bufferSize);
                ByteBuf partitionedBuffer = ALLOCATOR.buffer(endPosition - position);
                partitionedBuffer.writeBytes(buffer, position, endPosition - position);
                result.add(partitionedBuffer);
            }
        } catch (Throwable t) {
            releaseBuffers(result.toArray(new ByteBuf[0]));
            ExceptionUtils.rethrow(t);
        }

        return result;
    }

    private List<NettyMessage> decodeMessages(EmbeddedChannel channel, List<ByteBuf> inputBuffers) {
        for (ByteBuf buffer : inputBuffers) {
            channel.writeInbound(buffer);
        }

        channel.runPendingTasks();

        List<NettyMessage> decodedMessages = new ArrayList<>();
        Object input;
        while ((input = channel.readInbound()) != null) {
            assertTrue(input instanceof NettyMessage);
            decodedMessages.add((NettyMessage) input);
        }

        return decodedMessages;
    }

    private void verifyDecodedMessages(
            List<BufferResponse> expectedMessages, List<NettyMessage> decodedMessages) {
        assertEquals(expectedMessages.size(), decodedMessages.size());
        for (int i = 0; i < expectedMessages.size(); ++i) {
            assertEquals(expectedMessages.get(i).getClass(), decodedMessages.get(i).getClass());

            BufferResponse expected = expectedMessages.get(i);
            BufferResponse actual = (BufferResponse) decodedMessages.get(i);
            verifyBufferResponseHeader(expected, actual);
            if (expected.bufferSize == 0 || !expected.receiverId.equals(inputChannelId)) {
                assertNull(actual.getBuffer());
            } else {
                assertEquals(expected.getBuffer(), actual.getBuffer());
            }
        }
    }

    private void releaseBuffers(@Nullable ByteBuf... buffers) {
        if (buffers != null) {
            for (ByteBuf buffer : buffers) {
                if (buffer != null) {
                    buffer.release();
                }
            }
        }
    }
}
