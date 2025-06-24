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
import org.apache.flink.runtime.io.network.buffer.FullyFilledBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyBufferResponseHeader;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests the client side message decoder. */
class NettyMessageClientDecoderDelegateTest {

    private static final int BUFFER_SIZE = 1024;

    private static final int NUMBER_OF_BUFFER_RESPONSES = 5;

    private static final NettyBufferPool ALLOCATOR = new NettyBufferPool(1);

    private EmbeddedChannel channel;

    private NetworkBufferPool networkBufferPool;

    private SingleInputGate inputGate;

    private InputChannelID inputChannelId;

    private InputChannelID releasedInputChannelId;

    private void setup(int numOfPartialBuffers) throws IOException, InterruptedException {
        CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        networkBufferPool =
                new NetworkBufferPool(
                        NUMBER_OF_BUFFER_RESPONSES, numOfPartialBuffers * BUFFER_SIZE);
        channel = new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));

        inputGate = createSingleInputGate(1, networkBufferPool);
        RemoteInputChannel inputChannel =
                createRemoteInputChannel(
                        inputGate, new TestingPartitionRequestClient(), NUMBER_OF_BUFFER_RESPONSES);
        inputGate.setInputChannels(inputChannel);
        inputGate.setup();
        inputChannel.requestSubpartitions();
        handler.addInputChannel(inputChannel);
        inputChannelId = inputChannel.getInputChannelId();

        SingleInputGate releasedInputGate = createSingleInputGate(1, networkBufferPool);
        RemoteInputChannel releasedInputChannel =
                new InputChannelBuilder().buildRemoteChannel(inputGate);
        releasedInputGate.close();
        handler.addInputChannel(releasedInputChannel);
        releasedInputChannelId = releasedInputChannel.getInputChannelId();
    }

    @AfterEach
    void tearDown() throws IOException {
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

    private static Stream<Arguments> bufferDescriptors() {
        return Stream.of(
                Arguments.of(false, 1), // Scenario with a regular Buffer
                Arguments.of(true, 1), // FullyFilledBuffer with 1 partial buffer
                Arguments.of(true, 3) // FullyFilledBuffer with 3 partial buffers
                );
    }

    /** Verifies that the client side decoder works well for unreleased input channels. */
    @ParameterizedTest(name = "{index} => isFullyFilled={0}, numOfPartialBuffers={1}")
    @MethodSource("bufferDescriptors")
    void testClientMessageDecode(boolean isFullyFilled, int numOfPartialBuffers) throws Exception {
        setup(numOfPartialBuffers);
        testNettyMessageClientDecoding(isFullyFilled, numOfPartialBuffers, false, false, false);
    }

    /**
     * Verifies that the client side decoder works well for empty buffers. Empty buffers should not
     * consume data buffers of the input channels.
     */
    @ParameterizedTest(name = "{index} => isFullyFilled={0}, numOfPartialBuffers={1}")
    @MethodSource("bufferDescriptors")
    void testClientMessageDecodeWithEmptyBuffers(boolean isFullyFilled, int numOfPartialBuffers)
            throws Exception {
        setup(numOfPartialBuffers);
        testNettyMessageClientDecoding(isFullyFilled, numOfPartialBuffers, true, false, false);
    }

    /**
     * Verifies that the client side decoder works well with buffers sent to a released input
     * channel. The data buffer part should be discarded before reading the next message.
     */
    @ParameterizedTest(name = "{index} => isFullyFilled={0}, numOfPartialBuffers={1}")
    @MethodSource("bufferDescriptors")
    void testClientMessageDecodeWithReleasedInputChannel(
            boolean isFullyFilled, int numOfPartialBuffers) throws Exception {
        setup(numOfPartialBuffers);
        testNettyMessageClientDecoding(isFullyFilled, numOfPartialBuffers, false, true, false);
    }

    /**
     * Verifies that the client side decoder works well with buffers sent to a removed input
     * channel. The data buffer part should be discarded before reading the next message.
     */
    @ParameterizedTest(name = "{index} => isFullyFilled={0}, numOfPartialBuffers={1}")
    @MethodSource("bufferDescriptors")
    void testClientMessageDecodeWithRemovedInputChannel(
            boolean isFullyFilled, int numOfPartialBuffers) throws Exception {
        setup(numOfPartialBuffers);
        testNettyMessageClientDecoding(isFullyFilled, numOfPartialBuffers, false, false, true);
    }

    // ------------------------------------------------------------------------------------------------------------------

    private void testNettyMessageClientDecoding(
            boolean isFullyFilled,
            int numOfPartialBuffers,
            boolean hasEmptyBuffer,
            boolean hasBufferForReleasedChannel,
            boolean hasBufferForRemovedChannel)
            throws Exception {

        ByteBuf[] encodedMessages = null;
        List<NettyMessage> decodedMessages = null;
        try {
            List<BufferResponse> messages =
                    createMessageList(
                            isFullyFilled,
                            numOfPartialBuffers,
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
            boolean isFullyFilled,
            int numOfPartialBuffers,
            boolean hasEmptyBuffer,
            boolean hasBufferForRemovedChannel,
            boolean hasBufferForReleasedChannel) {

        int seqNumber = 1;
        List<BufferResponse> messages = new ArrayList<>();

        for (int i = 0; i < NUMBER_OF_BUFFER_RESPONSES - 1; i++) {
            addBufferResponse(
                    isFullyFilled,
                    numOfPartialBuffers,
                    messages,
                    inputChannelId,
                    Buffer.DataType.DATA_BUFFER,
                    BUFFER_SIZE,
                    seqNumber++);
        }

        if (hasEmptyBuffer) {
            addBufferResponse(
                    isFullyFilled,
                    numOfPartialBuffers,
                    messages,
                    inputChannelId,
                    Buffer.DataType.DATA_BUFFER,
                    0,
                    seqNumber++);
        }
        if (hasBufferForReleasedChannel) {
            addBufferResponse(
                    isFullyFilled,
                    numOfPartialBuffers,
                    messages,
                    releasedInputChannelId,
                    Buffer.DataType.DATA_BUFFER,
                    BUFFER_SIZE,
                    seqNumber++);
        }
        if (hasBufferForRemovedChannel) {
            addBufferResponse(
                    isFullyFilled,
                    numOfPartialBuffers,
                    messages,
                    new InputChannelID(),
                    Buffer.DataType.DATA_BUFFER,
                    BUFFER_SIZE,
                    seqNumber++);
        }

        addBufferResponse(
                isFullyFilled,
                numOfPartialBuffers,
                messages,
                inputChannelId,
                Buffer.DataType.EVENT_BUFFER,
                32,
                seqNumber++);
        addBufferResponse(
                isFullyFilled,
                numOfPartialBuffers,
                messages,
                inputChannelId,
                Buffer.DataType.DATA_BUFFER,
                BUFFER_SIZE,
                seqNumber);

        return messages;
    }

    private void addBufferResponse(
            boolean isFullyFilled,
            int numOfPartialBuffers,
            List<BufferResponse> messages,
            InputChannelID inputChannelId,
            Buffer.DataType dataType,
            int bufferSize,
            int seqNumber) {

        Buffer buffer = createDataBuffer(isFullyFilled, numOfPartialBuffers, bufferSize, dataType);
        BufferResponse bufferResponse =
                new BufferResponse(
                        buffer,
                        seqNumber,
                        inputChannelId,
                        0,
                        isFullyFilled ? numOfPartialBuffers : 0,
                        1);
        if (isFullyFilled) {
            for (int i = 0; i < numOfPartialBuffers; i++) {
                bufferResponse.getPartialBufferSizes().add(bufferSize);
            }
        }
        messages.add(bufferResponse);
    }

    private Buffer createDataBuffer(
            boolean isFullyFilled, int numOfPartialBuffers, int size, Buffer.DataType dataType) {
        if (!isFullyFilled) {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(size);
            NetworkBuffer buffer =
                    new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType);
            for (int i = 0; i < size / 4; ++i) {
                buffer.writeInt(i);
            }

            return buffer;
        } else {
            FullyFilledBuffer fullyFilledBuffer =
                    new FullyFilledBuffer(dataType, numOfPartialBuffers * size, false);

            for (int i = 0; i < numOfPartialBuffers; i++) {
                MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(size);
                NetworkBuffer buffer =
                        new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, dataType);
                for (int num = 0; num < size / 4; ++num) {
                    buffer.writeInt(num);
                }

                fullyFilledBuffer.addPartialBuffer(buffer);
            }

            return fullyFilledBuffer;
        }
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
            assertThat(input).isInstanceOf(NettyMessage.class);
            decodedMessages.add((NettyMessage) input);
        }

        return decodedMessages;
    }

    private void verifyDecodedMessages(
            List<BufferResponse> expectedMessages, List<NettyMessage> decodedMessages) {
        assertThat(decodedMessages).hasSameSizeAs(expectedMessages);
        for (int i = 0; i < expectedMessages.size(); ++i) {
            assertThat(decodedMessages.get(i)).isInstanceOf(expectedMessages.get(i).getClass());

            BufferResponse expected = expectedMessages.get(i);
            BufferResponse actual = (BufferResponse) decodedMessages.get(i);
            verifyBufferResponseHeader(expected, actual);
            if (expected.bufferSize == 0 || !expected.receiverId.equals(inputChannelId)) {
                assertThat(actual.getBuffer()).isNull();
            } else {
                Buffer buffer = expected.getBuffer();
                if (buffer instanceof FullyFilledBuffer) {
                    assertThat(actual.getBuffer()).isEqualTo(buffer.asByteBuf());
                } else {
                    assertThat(actual.getBuffer()).isEqualTo(buffer);
                }
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
