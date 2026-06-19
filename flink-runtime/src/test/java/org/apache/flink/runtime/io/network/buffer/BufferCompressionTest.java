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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions.CompressionCodec;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BufferCompressor} and {@link BufferDecompressor}. */
@ExtendWith(ParameterizedTestExtension.class)
class BufferCompressionTest {

    private static final int BUFFER_SIZE = 4 * 1024 * 1024;

    private static final int NUM_LONGS = BUFFER_SIZE / 8;

    private final boolean compressToOriginalBuffer;

    private final boolean decompressToOriginalBuffer;

    private final BufferCompressor compressor;

    private final BufferDecompressor decompressor;

    private final Buffer bufferToCompress;

    @Parameters(
            name =
                    "isDirect = {0}, codec = {1}, compressToOriginal = {2}, decompressToOriginal = {3}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
                new Object[][] {
                    {true, CompressionCodec.LZ4, true, false},
                    {true, CompressionCodec.LZ4, false, true},
                    {true, CompressionCodec.LZ4, false, false},
                    {false, CompressionCodec.LZ4, true, false},
                    {false, CompressionCodec.LZ4, false, true},
                    {false, CompressionCodec.LZ4, false, false},
                    {true, CompressionCodec.ZSTD, true, false},
                    {true, CompressionCodec.ZSTD, false, true},
                    {true, CompressionCodec.ZSTD, false, false},
                    {false, CompressionCodec.ZSTD, true, false},
                    {false, CompressionCodec.ZSTD, false, true},
                    {false, CompressionCodec.ZSTD, false, false},
                    {true, CompressionCodec.LZO, true, false},
                    {true, CompressionCodec.LZO, false, true},
                    {true, CompressionCodec.LZO, false, false},
                    {false, CompressionCodec.LZO, true, false},
                    {false, CompressionCodec.LZO, false, true},
                    {false, CompressionCodec.LZO, false, false}
                });
    }

    public BufferCompressionTest(
            boolean isDirect,
            CompressionCodec compressionCodec,
            boolean compressToOriginalBuffer,
            boolean decompressToOriginalBuffer) {
        this.compressToOriginalBuffer = compressToOriginalBuffer;
        this.decompressToOriginalBuffer = decompressToOriginalBuffer;
        this.compressor = new BufferCompressor(BUFFER_SIZE, compressionCodec);
        this.decompressor = new BufferDecompressor(BUFFER_SIZE, compressionCodec);
        this.bufferToCompress = createBufferAndFillWithLongValues(isDirect);
    }

    @TestTemplate
    void testCompressAndDecompressNetWorkBuffer() {
        Buffer compressedBuffer = compress(compressor, bufferToCompress, compressToOriginalBuffer);
        assertThat(compressedBuffer.isCompressed()).isTrue();

        Buffer decompressedBuffer =
                decompress(decompressor, compressedBuffer, decompressToOriginalBuffer);
        assertThat(decompressedBuffer.isCompressed()).isFalse();

        verifyDecompressionResult(decompressedBuffer, 0, NUM_LONGS);
    }

    @TestTemplate
    void testCompressAndDecompressReadOnlySlicedNetworkBuffer() {
        int offset = NUM_LONGS / 4 * 8;
        int length = NUM_LONGS / 2 * 8;

        Buffer readOnlySlicedBuffer = bufferToCompress.readOnlySlice(offset, length);
        Buffer compressedBuffer =
                compress(compressor, readOnlySlicedBuffer, compressToOriginalBuffer);
        assertThat(compressedBuffer.isCompressed()).isTrue();

        Buffer decompressedBuffer =
                decompress(decompressor, compressedBuffer, decompressToOriginalBuffer);
        assertThat(decompressedBuffer.isCompressed()).isFalse();

        verifyDecompressionResult(decompressedBuffer, NUM_LONGS / 4, NUM_LONGS / 2);
    }

    @TestTemplate
    void testCompressEmptyBuffer() {
        assertThatThrownBy(
                        () ->
                                compress(
                                        compressor,
                                        bufferToCompress.readOnlySlice(0, 0),
                                        compressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testDecompressEmptyBuffer() {
        Buffer readOnlySlicedBuffer = bufferToCompress.readOnlySlice(0, 0);
        readOnlySlicedBuffer.setCompressed(true);

        assertThatThrownBy(
                        () ->
                                decompress(
                                        decompressor,
                                        readOnlySlicedBuffer,
                                        decompressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testCompressBufferWithNonZeroReadOffset() {
        bufferToCompress.setReaderIndex(1);

        assertThatThrownBy(() -> compress(compressor, bufferToCompress, compressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testDecompressBufferWithNonZeroReadOffset() {
        bufferToCompress.setReaderIndex(1);
        bufferToCompress.setCompressed(true);

        assertThatThrownBy(
                        () ->
                                decompress(
                                        decompressor, bufferToCompress, decompressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testCompressNull() {
        assertThatThrownBy(() -> compress(compressor, null, compressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testDecompressNull() {
        assertThatThrownBy(() -> decompress(decompressor, null, decompressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testCompressCompressedBuffer() {
        bufferToCompress.setCompressed(true);

        assertThatThrownBy(() -> compress(compressor, bufferToCompress, compressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testDecompressUncompressedBuffer() {
        assertThatThrownBy(
                        () ->
                                decompress(
                                        decompressor, bufferToCompress, decompressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testCompressEvent() {
        assertThatThrownBy(
                        () ->
                                compress(
                                        compressor,
                                        EventSerializer.toBuffer(
                                                EndOfPartitionEvent.INSTANCE, false),
                                        compressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testDecompressEvent() {
        assertThatThrownBy(
                        () ->
                                decompress(
                                        decompressor,
                                        EventSerializer.toBuffer(
                                                EndOfPartitionEvent.INSTANCE, false),
                                        decompressToOriginalBuffer))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testDataSizeGrowsAfterCompression() {
        int numBytes = 1;
        Buffer readOnlySlicedBuffer = bufferToCompress.readOnlySlice(BUFFER_SIZE / 2, numBytes);

        Buffer compressedBuffer =
                compress(compressor, readOnlySlicedBuffer, compressToOriginalBuffer);
        assertThat(compressedBuffer.isCompressed()).isFalse();
        assertThat(compressedBuffer).isEqualTo(readOnlySlicedBuffer);
        assertThat(compressedBuffer.readableBytes()).isEqualTo(numBytes);
    }

    private static Buffer createBufferAndFillWithLongValues(boolean isDirect) {
        MemorySegment segment;
        if (isDirect) {
            segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
        } else {
            segment = MemorySegmentFactory.allocateUnpooledOffHeapMemory(BUFFER_SIZE);
        }
        for (int i = 0; i < NUM_LONGS; ++i) {
            segment.putLongLittleEndian(8 * i, i);
        }
        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        buffer.setSize(8 * NUM_LONGS);
        return buffer;
    }

    private static void verifyDecompressionResult(Buffer buffer, long start, int numLongs) {
        ByteBuffer byteBuffer = buffer.getNioBufferReadable().order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < numLongs; ++i) {
            assertThat(byteBuffer.getLong()).isEqualTo(start + i);
        }
    }

    private static Buffer compress(
            BufferCompressor compressor, Buffer buffer, boolean compressToOriginalBuffer) {
        if (compressToOriginalBuffer) {
            return compressor.compressToOriginalBuffer(buffer);
        }
        return compressor.compressToIntermediateBuffer(buffer);
    }

    private static Buffer decompress(
            BufferDecompressor decompressor, Buffer buffer, boolean decompressToOriginalBuffer) {
        if (decompressToOriginalBuffer) {
            return decompressor.decompressToOriginalBuffer(buffer);
        }
        return decompressor.decompressToIntermediateBuffer(buffer);
    }
}
