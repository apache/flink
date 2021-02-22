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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/** Tests for {@link BufferCompressor} and {@link BufferDecompressor}. */
@RunWith(Parameterized.class)
public class BufferCompressionTest {

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
                    {true, "LZ4", true, false},
                    {true, "LZ4", false, true},
                    {true, "LZ4", false, false},
                    {false, "LZ4", true, false},
                    {false, "LZ4", false, true},
                    {false, "LZ4", false, false},
                });
    }

    public BufferCompressionTest(
            boolean isDirect,
            String compressionCodec,
            boolean compressToOriginalBuffer,
            boolean decompressToOriginalBuffer) {
        this.compressToOriginalBuffer = compressToOriginalBuffer;
        this.decompressToOriginalBuffer = decompressToOriginalBuffer;
        this.compressor = new BufferCompressor(BUFFER_SIZE, compressionCodec);
        this.decompressor = new BufferDecompressor(BUFFER_SIZE, compressionCodec);
        this.bufferToCompress = createBufferAndFillWithLongValues(isDirect);
    }

    @Test
    public void testCompressAndDecompressNetWorkBuffer() {
        Buffer compressedBuffer = compress(compressor, bufferToCompress, compressToOriginalBuffer);
        assertTrue(compressedBuffer.isCompressed());

        Buffer decompressedBuffer =
                decompress(decompressor, compressedBuffer, decompressToOriginalBuffer);
        assertFalse(decompressedBuffer.isCompressed());

        verifyDecompressionResult(decompressedBuffer, 0, NUM_LONGS);
    }

    @Test
    public void testCompressAndDecompressReadOnlySlicedNetworkBuffer() {
        int offset = NUM_LONGS / 4 * 8;
        int length = NUM_LONGS / 2 * 8;

        Buffer readOnlySlicedBuffer = bufferToCompress.readOnlySlice(offset, length);
        Buffer compressedBuffer =
                compress(compressor, readOnlySlicedBuffer, compressToOriginalBuffer);
        assertTrue(compressedBuffer.isCompressed());

        Buffer decompressedBuffer =
                decompress(decompressor, compressedBuffer, decompressToOriginalBuffer);
        assertFalse(decompressedBuffer.isCompressed());

        verifyDecompressionResult(decompressedBuffer, NUM_LONGS / 4, NUM_LONGS / 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompressEmptyBuffer() {
        compress(compressor, bufferToCompress.readOnlySlice(0, 0), compressToOriginalBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecompressEmptyBuffer() {
        Buffer readOnlySlicedBuffer = bufferToCompress.readOnlySlice(0, 0);
        readOnlySlicedBuffer.setCompressed(true);

        decompress(decompressor, readOnlySlicedBuffer, decompressToOriginalBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompressBufferWithNonZeroReadOffset() {
        bufferToCompress.setReaderIndex(1);

        compress(compressor, bufferToCompress, compressToOriginalBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecompressBufferWithNonZeroReadOffset() {
        bufferToCompress.setReaderIndex(1);
        bufferToCompress.setCompressed(true);

        decompress(decompressor, bufferToCompress, decompressToOriginalBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompressNull() {
        compress(compressor, null, compressToOriginalBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecompressNull() {
        decompress(decompressor, null, decompressToOriginalBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompressCompressedBuffer() {
        bufferToCompress.setCompressed(true);

        compress(compressor, bufferToCompress, compressToOriginalBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecompressUncompressedBuffer() {
        decompress(decompressor, bufferToCompress, decompressToOriginalBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCompressEvent() throws IOException {
        compress(
                compressor,
                EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false),
                compressToOriginalBuffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDecompressEvent() throws IOException {
        decompress(
                decompressor,
                EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false),
                decompressToOriginalBuffer);
    }

    @Test
    public void testDataSizeGrowsAfterCompression() {
        int numBytes = 1;
        Buffer readOnlySlicedBuffer = bufferToCompress.readOnlySlice(BUFFER_SIZE / 2, numBytes);

        Buffer compressedBuffer =
                compress(compressor, readOnlySlicedBuffer, compressToOriginalBuffer);
        assertFalse(compressedBuffer.isCompressed());
        assertEquals(readOnlySlicedBuffer, compressedBuffer);
        assertEquals(numBytes, compressedBuffer.readableBytes());
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
            assertEquals(start + i, byteBuffer.getLong());
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
