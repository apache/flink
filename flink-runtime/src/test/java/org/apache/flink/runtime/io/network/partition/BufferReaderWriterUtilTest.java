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

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/** Tests for the {@link BufferReaderWriterUtil}. */
public class BufferReaderWriterUtilTest {

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    // ------------------------------------------------------------------------
    // Byte Buffer
    // ------------------------------------------------------------------------

    @Test
    public void writeReadByteBuffer() {
        final ByteBuffer memory = ByteBuffer.allocateDirect(1200);
        final Buffer buffer = createTestBuffer();

        BufferReaderWriterUtil.writeBuffer(buffer, memory);
        final int pos = memory.position();
        memory.flip();
        Buffer result = BufferReaderWriterUtil.sliceNextBuffer(memory);

        assertEquals(pos, memory.position());
        validateTestBuffer(result);
    }

    @Test
    public void writeByteBufferNotEnoughSpace() {
        final ByteBuffer memory = ByteBuffer.allocateDirect(10);
        final Buffer buffer = createTestBuffer();

        final boolean written = BufferReaderWriterUtil.writeBuffer(buffer, memory);

        assertFalse(written);
        assertEquals(0, memory.position());
        assertEquals(memory.capacity(), memory.limit());
    }

    @Test
    public void readFromEmptyByteBuffer() {
        final ByteBuffer memory = ByteBuffer.allocateDirect(100);
        memory.position(memory.limit());

        final Buffer result = BufferReaderWriterUtil.sliceNextBuffer(memory);

        assertNull(result);
    }

    @Test
    public void testReadFromByteBufferNotEnoughData() {
        final ByteBuffer memory = ByteBuffer.allocateDirect(1200);
        final Buffer buffer = createTestBuffer();
        BufferReaderWriterUtil.writeBuffer(buffer, memory);

        memory.flip().limit(memory.limit() - 1);
        ByteBuffer tooSmall = memory.slice();

        try {
            BufferReaderWriterUtil.sliceNextBuffer(tooSmall);
            fail();
        } catch (Exception e) {
            // expected
        }
    }

    // ------------------------------------------------------------------------
    //  File Channel
    // ------------------------------------------------------------------------

    @Test
    public void writeReadFileChannel() throws Exception {
        final FileChannel fc = tmpFileChannel();
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
    public void readPrematureEndOfFile1() throws Exception {
        final FileChannel fc = tmpFileChannel();
        final Buffer buffer = createTestBuffer();
        final MemorySegment readBuffer =
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(buffer.getSize(), null);

        BufferReaderWriterUtil.writeToByteChannel(
                fc, buffer, BufferReaderWriterUtil.allocatedWriteBufferArray());
        fc.truncate(fc.position() - 1);
        fc.position(0);

        try {
            BufferReaderWriterUtil.readFromByteChannel(
                    fc,
                    BufferReaderWriterUtil.allocatedHeaderBuffer(),
                    readBuffer,
                    FreeingBufferRecycler.INSTANCE);
            fail();
        } catch (IOException e) {
            // expected
        }
    }

    @Test
    public void readPrematureEndOfFile2() throws Exception {
        final FileChannel fc = tmpFileChannel();
        final Buffer buffer = createTestBuffer();
        final MemorySegment readBuffer =
                MemorySegmentFactory.allocateUnpooledOffHeapMemory(buffer.getSize(), null);

        BufferReaderWriterUtil.writeToByteChannel(
                fc, buffer, BufferReaderWriterUtil.allocatedWriteBufferArray());
        fc.truncate(2); // less than a header size
        fc.position(0);

        try {
            BufferReaderWriterUtil.readFromByteChannel(
                    fc,
                    BufferReaderWriterUtil.allocatedHeaderBuffer(),
                    readBuffer,
                    FreeingBufferRecycler.INSTANCE);
            fail();
        } catch (IOException e) {
            // expected
        }
    }

    @Test
    public void testBulkWritingLargeNumberOfBuffers() throws Exception {
        int bufferSize = 1024;
        int numBuffers = 1025;
        try (FileChannel fileChannel = tmpFileChannel()) {
            ByteBuffer[] data = new ByteBuffer[numBuffers];
            for (int i = 0; i < numBuffers; ++i) {
                data[i] = ByteBuffer.allocateDirect(bufferSize);
            }
            int bytesExpected = bufferSize * numBuffers;
            BufferReaderWriterUtil.writeBuffers(fileChannel, bytesExpected, data);
            assertEquals(bytesExpected, fileChannel.size());
        }
    }

    // ------------------------------------------------------------------------
    //  Mixed
    // ------------------------------------------------------------------------

    @Test
    public void writeFileReadMemoryBuffer() throws Exception {
        final FileChannel fc = tmpFileChannel();
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

    private static FileChannel tmpFileChannel() throws IOException {
        return FileChannel.open(
                TMP_FOLDER.newFile().toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    private static Buffer createTestBuffer() {
        return BufferBuilderTestUtils.buildBufferWithAscendingInts(1024, 200, 0);
    }

    private static void validateTestBuffer(Buffer buffer) {
        BufferBuilderTestUtils.validateBufferWithAscendingInts(buffer, 200, 0);
    }
}
