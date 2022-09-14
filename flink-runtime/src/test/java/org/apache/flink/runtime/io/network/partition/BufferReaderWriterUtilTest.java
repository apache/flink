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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link BufferReaderWriterUtil}. */
@ExtendWith(TestLoggerExtension.class)
class BufferReaderWriterUtilTest {

    // ------------------------------------------------------------------------
    // Byte Buffer
    // ------------------------------------------------------------------------

    @Test
    void writeReadByteBuffer() {
        final ByteBuffer memory = ByteBuffer.allocateDirect(1200);
        final Buffer buffer = createTestBuffer();
        BufferReaderWriterUtil.configureByteBuffer(memory);

        BufferReaderWriterUtil.writeBuffer(buffer, memory);
        final int pos = memory.position();
        memory.flip();
        Buffer result = BufferReaderWriterUtil.sliceNextBuffer(memory);

        assertThat(memory.position()).isEqualTo(pos);
        validateTestBuffer(result);
    }

    @Test
    void writeByteBufferNotEnoughSpace() {
        final ByteBuffer memory = ByteBuffer.allocateDirect(10);
        final Buffer buffer = createTestBuffer();

        final boolean written = BufferReaderWriterUtil.writeBuffer(buffer, memory);

        assertThat(written).isFalse();
        assertThat(memory.position()).isZero();
        assertThat(memory.limit()).isEqualTo(memory.capacity());
    }

    @Test
    void readFromEmptyByteBuffer() {
        final ByteBuffer memory = ByteBuffer.allocateDirect(100);
        memory.position(memory.limit());

        final Buffer result = BufferReaderWriterUtil.sliceNextBuffer(memory);

        assertThat(result).isNull();
    }

    @Test
    void testReadFromByteBufferNotEnoughData() {
        final ByteBuffer memory = ByteBuffer.allocateDirect(1200);
        final Buffer buffer = createTestBuffer();
        BufferReaderWriterUtil.writeBuffer(buffer, memory);

        memory.flip().limit(memory.limit() - 1);
        ByteBuffer tooSmall = memory.slice();

        assertThatThrownBy(() -> BufferReaderWriterUtil.sliceNextBuffer(tooSmall))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------------
    //  File Channel
    // ------------------------------------------------------------------------

    @Test
    void writeReadFileChannel(@TempDir Path tempPath) throws Exception {
        final FileChannel fc = tmpFileChannel(tempPath);
        final Buffer buffer = createTestBuffer();
        final MemorySegment readBuffer =
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(buffer.getSize(), null);

        BufferReaderWriterUtil.writeToByteChannel(
                fc, buffer, BufferReaderWriterUtil.allocatedWriteBufferArray());
        fc.position(0);

        Buffer result =
                BufferReaderWriterUtil.readFromByteChannel(
                        fc,
                        BufferReaderWriterUtil.allocatedHeaderBuffer(),
                        readBuffer,
                        FreeingBufferRecycler.INSTANCE);

        validateTestBuffer(result);
    }

    @Test
    void readPrematureEndOfFile1(@TempDir Path tempPath) throws Exception {
        final FileChannel fc = tmpFileChannel(tempPath);
        final Buffer buffer = createTestBuffer();
        final MemorySegment readBuffer =
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(buffer.getSize(), null);

        BufferReaderWriterUtil.writeToByteChannel(
                fc, buffer, BufferReaderWriterUtil.allocatedWriteBufferArray());
        fc.truncate(fc.position() - 1);
        fc.position(0);

        assertThatThrownBy(
                        () ->
                                BufferReaderWriterUtil.readFromByteChannel(
                                        fc,
                                        BufferReaderWriterUtil.allocatedHeaderBuffer(),
                                        readBuffer,
                                        FreeingBufferRecycler.INSTANCE))
                .isInstanceOf(IOException.class);
    }

    @Test
    void readPrematureEndOfFile2(@TempDir Path tempPath) throws Exception {
        final FileChannel fc = tmpFileChannel(tempPath);
        final Buffer buffer = createTestBuffer();
        final MemorySegment readBuffer =
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(buffer.getSize(), null);

        BufferReaderWriterUtil.writeToByteChannel(
                fc, buffer, BufferReaderWriterUtil.allocatedWriteBufferArray());
        fc.truncate(2); // less than a header size
        fc.position(0);

        assertThatThrownBy(
                        () ->
                                BufferReaderWriterUtil.readFromByteChannel(
                                        fc,
                                        BufferReaderWriterUtil.allocatedHeaderBuffer(),
                                        readBuffer,
                                        FreeingBufferRecycler.INSTANCE))
                .isInstanceOf(IOException.class);
    }

    @Test
    void testBulkWritingLargeNumberOfBuffers(@TempDir Path tempPath) throws Exception {
        int bufferSize = 1024;
        int numBuffers = 1025;
        try (FileChannel fileChannel = tmpFileChannel(tempPath)) {
            ByteBuffer[] data = new ByteBuffer[numBuffers];
            for (int i = 0; i < numBuffers; ++i) {
                data[i] = ByteBuffer.allocateDirect(bufferSize);
            }
            int bytesExpected = bufferSize * numBuffers;
            BufferReaderWriterUtil.writeBuffers(fileChannel, bytesExpected, data);
            assertThat(fileChannel.size()).isEqualTo(bytesExpected);
        }
    }

    @Test
    void testPositionToNextBuffer(@TempDir Path tempPath) throws Exception {
        final FileChannel fc = tmpFileChannel(tempPath);
        ByteBuffer[] byteBuffersWithHeader = createByteBuffersWithHeader(2);
        long totalBytes =
                Arrays.stream(byteBuffersWithHeader).mapToLong(ByteBuffer::remaining).sum();
        BufferReaderWriterUtil.writeBuffers(fc, totalBytes, byteBuffersWithHeader);
        // reset the channel's position to read.
        fc.position(0);
        BufferReaderWriterUtil.positionToNextBuffer(fc, byteBuffersWithHeader[0]);
        long expectedPosition = totalBytes / 2;
        assertThat(fc.position()).isEqualTo(expectedPosition);
    }

    // ------------------------------------------------------------------------
    //  Mixed
    // ------------------------------------------------------------------------

    @Test
    void writeFileReadMemoryBuffer(@TempDir Path tempPath) throws Exception {
        final FileChannel fc = tmpFileChannel(tempPath);
        final Buffer buffer = createTestBuffer();
        BufferReaderWriterUtil.writeToByteChannel(
                fc, buffer, BufferReaderWriterUtil.allocatedWriteBufferArray());

        final ByteBuffer bb =
                fc.map(MapMode.READ_ONLY, 0, fc.position()).order(ByteOrder.nativeOrder());
        BufferReaderWriterUtil.configureByteBuffer(bb);
        fc.close();

        Buffer result = BufferReaderWriterUtil.sliceNextBuffer(bb);

        validateTestBuffer(result);
    }

    // ------------------------------------------------------------------------
    //  Util
    // ------------------------------------------------------------------------

    private static FileChannel tmpFileChannel(Path tempPath) throws IOException {
        return FileChannel.open(
                Files.createFile(tempPath.resolve(UUID.randomUUID().toString())),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    /**
     * Create an array of ByteBuffer, the odd-numbered position in the array is header buffer, and
     * the even-numbered position is the corresponding data buffer.
     */
    private static ByteBuffer[] createByteBuffersWithHeader(int numBuffers) {
        ByteBuffer[] buffers = new ByteBuffer[numBuffers * 2];
        for (int i = 0; i < numBuffers; i++) {
            buffers[2 * i] = BufferReaderWriterUtil.allocatedHeaderBuffer();
            Buffer buffer = createTestBuffer();
            BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, buffers[2 * i]);
            buffers[2 * i + 1] = buffer.getNioBufferReadable();
        }
        return buffers;
    }

    private static Buffer createTestBuffer() {
        return BufferBuilderTestUtils.buildBufferWithAscendingInts(1024, 200, 0);
    }

    private static void validateTestBuffer(Buffer buffer) {
        BufferBuilderTestUtils.validateBufferWithAscendingInts(buffer, 200, 0);
    }
}
