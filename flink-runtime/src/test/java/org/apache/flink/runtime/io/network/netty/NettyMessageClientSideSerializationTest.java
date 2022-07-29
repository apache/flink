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
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Random;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BacklogAnnouncement;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.NettyMessageEncoder;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.encodeAndDecode;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyBufferResponseHeader;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyErrorResponse;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the serialization and deserialization of the various {@link NettyMessage} sub-classes
 * sent from server side to client side.
 */
@ExtendWith(TestLoggerExtension.class)
class NettyMessageClientSideSerializationTest {

    private static final int BUFFER_SIZE = 1024;

    private final Random random = new Random();
    private static BufferCompressor compressor;

    private static BufferDecompressor decompressor;

    private EmbeddedChannel channel;

    private NetworkBufferPool networkBufferPool;

    private SingleInputGate inputGate;

    private InputChannelID inputChannelId;

    @BeforeEach
    void setup() throws IOException, InterruptedException {
        networkBufferPool = new NetworkBufferPool(8, BUFFER_SIZE);
        inputGate = createSingleInputGate(1, networkBufferPool);
        RemoteInputChannel inputChannel =
                createRemoteInputChannel(inputGate, new TestingPartitionRequestClient());
        inputChannel.requestSubpartition();
        inputGate.setInputChannels(inputChannel);
        inputGate.setup();

        CreditBasedPartitionRequestClientHandler handler =
                new CreditBasedPartitionRequestClientHandler();
        handler.addInputChannel(inputChannel);

        channel =
                new EmbeddedChannel(
                        new NettyMessageEncoder(), // For outbound messages
                        new NettyMessageClientDecoderDelegate(handler)); // For inbound messages

        inputChannelId = inputChannel.getInputChannelId();
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

    @Test
    void testErrorResponseWithoutErrorMessage() {
        testErrorResponse(new ErrorResponse(new IllegalStateException(), inputChannelId));
    }

    @Test
    void testErrorResponseWithErrorMessage() {
        testErrorResponse(
                new ErrorResponse(
                        new IllegalStateException("Illegal illegal illegal"), inputChannelId));
    }

    @Test
    void testErrorResponseWithFatalError() {
        testErrorResponse(new ErrorResponse(new IllegalStateException("Illegal illegal illegal")));
    }

    @Test
    void testOrdinaryBufferResponse() {
        testBufferResponse(false, false);
    }

    @Test
    void testBufferResponseWithReadOnlySlice() {
        testBufferResponse(true, false);
    }

    @ParameterizedTest
    @ValueSource(strings = {"LZ4", "LZO", "ZSTD"})
    void testCompressedBufferResponse(final String codecFactoryName) {
        compressor = new BufferCompressor(BUFFER_SIZE, codecFactoryName);
        decompressor = new BufferDecompressor(BUFFER_SIZE, codecFactoryName);
        testBufferResponse(false, true);
    }

    @Test
    void testBacklogAnnouncement() {
        BacklogAnnouncement expected = new BacklogAnnouncement(1024, inputChannelId);
        BacklogAnnouncement actual = encodeAndDecode(expected, channel);
        assertThat(actual.backlog).isEqualTo(expected.backlog);
        assertThat(actual.receiverId).isEqualTo(expected.receiverId);
    }

    private void testErrorResponse(ErrorResponse expect) {
        ErrorResponse actual = encodeAndDecode(expect, channel);
        verifyErrorResponse(expect, actual);
    }

    private void testBufferResponse(boolean testReadOnlyBuffer, boolean testCompressedBuffer) {
        checkArgument(
                !(testReadOnlyBuffer & testCompressedBuffer),
                "There are no cases with both readonly slice and compression.");

        NetworkBuffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE),
                        FreeingBufferRecycler.INSTANCE);
        for (int i = 0; i < BUFFER_SIZE; i += 8) {
            buffer.writeLong(i);
        }

        Buffer testBuffer = buffer;
        if (testReadOnlyBuffer) {
            testBuffer = buffer.readOnlySlice();
        } else if (testCompressedBuffer) {
            testBuffer = compressor.compressToOriginalBuffer(buffer);
        }

        BufferResponse expected =
                new BufferResponse(
                        testBuffer,
                        random.nextInt(Integer.MAX_VALUE),
                        inputChannelId,
                        random.nextInt(Integer.MAX_VALUE));
        BufferResponse actual = encodeAndDecode(expected, channel);

        assertThat(buffer.isRecycled()).isTrue();
        assertThat(testBuffer.isRecycled()).isTrue();
        assertThat(actual.getBuffer())
                .as("The request input channel should always have available buffers in this test.")
                .isNotNull();

        Buffer decodedBuffer = actual.getBuffer();
        if (testCompressedBuffer) {
            assertThat(actual.isCompressed).isTrue();
            decodedBuffer = decompress(decodedBuffer);
        }

        verifyBufferResponseHeader(expected, actual);
        assertThat(decodedBuffer.readableBytes()).isEqualTo(BUFFER_SIZE);
        for (int i = 0; i < BUFFER_SIZE; i += 8) {
            assertThat(decodedBuffer.asByteBuf().readLong()).isEqualTo(i);
        }

        // Release the received message.
        actual.releaseBuffer();
        if (testCompressedBuffer) {
            decodedBuffer.recycleBuffer();
        }

        assertThat(actual.getBuffer().isRecycled()).isTrue();
    }

    private Buffer decompress(Buffer buffer) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
        Buffer compressedBuffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        buffer.asByteBuf().readBytes(compressedBuffer.asByteBuf(), buffer.readableBytes());
        compressedBuffer.setCompressed(true);
        return decompressor.decompressToOriginalBuffer(compressedBuffer);
    }
}
