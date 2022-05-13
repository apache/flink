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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BufferFileWriterReaderTest {

    private static final int BUFFER_SIZE = 32 * 1024;

    private static final BufferRecycler BUFFER_RECYCLER = FreeingBufferRecycler.INSTANCE;

    private static final Random random = new Random();

    private static final IOManager ioManager = new IOManagerAsync();

    private BufferFileWriter writer;

    private BufferFileReader reader;

    private LinkedBlockingQueue<Buffer> returnedBuffers = new LinkedBlockingQueue<>();

    @AfterClass
    public static void shutdown() throws Exception {
        ioManager.close();
    }

    @Before
    public void setUpWriterAndReader() {
        final FileIOChannel.ID channel = ioManager.createChannel();

        try {
            writer = ioManager.createBufferFileWriter(channel);
            reader =
                    ioManager.createBufferFileReader(
                            channel, new QueuingCallback<>(returnedBuffers));
        } catch (IOException e) {
            if (writer != null) {
                writer.deleteChannel();
            }

            if (reader != null) {
                reader.deleteChannel();
            }

            fail("Failed to setup writer and reader.");
        }
    }

    @After
    public void tearDownWriterAndReader() {
        if (writer != null) {
            writer.deleteChannel();
        }

        if (reader != null) {
            reader.deleteChannel();
        }

        returnedBuffers.clear();
    }

    @Test
    public void testWriteRead() throws IOException {
        int numBuffers = 1024;
        int currentNumber = 0;

        final int minBufferSize = BUFFER_SIZE / 4;

        // Write buffers filled with ascending numbers...
        for (int i = 0; i < numBuffers; i++) {
            final Buffer buffer = createBuffer();

            int size = getNextMultipleOf(getRandomNumberInRange(minBufferSize, BUFFER_SIZE), 4);

            currentNumber = fillBufferWithAscendingNumbers(buffer, currentNumber, size);

            writer.writeBlock(buffer);
        }

        // Make sure that the writes are finished
        writer.close();

        // Read buffers back in...
        for (int i = 0; i < numBuffers; i++) {
            assertFalse(reader.hasReachedEndOfFile());
            reader.readInto(createBuffer());
        }

        reader.close();

        assertTrue(reader.hasReachedEndOfFile());

        // Verify that the content is the same
        assertEquals("Read less buffers than written.", numBuffers, returnedBuffers.size());

        currentNumber = 0;
        Buffer buffer;

        while ((buffer = returnedBuffers.poll()) != null) {
            currentNumber = verifyBufferFilledWithAscendingNumbers(buffer, currentNumber);
        }
    }

    @Test
    public void testWriteSkipRead() throws IOException {
        int numBuffers = 1024;
        int currentNumber = 0;

        // Write buffers filled with ascending numbers...
        for (int i = 0; i < numBuffers; i++) {
            final Buffer buffer = createBuffer();

            currentNumber =
                    fillBufferWithAscendingNumbers(buffer, currentNumber, buffer.getMaxCapacity());

            writer.writeBlock(buffer);
        }

        // Make sure that the writes are finished
        writer.close();

        final int toSkip = 32;

        // Skip first buffers...
        reader.seekToPosition((8 + BUFFER_SIZE) * toSkip);

        numBuffers -= toSkip;

        // Read buffers back in...
        for (int i = 0; i < numBuffers; i++) {
            assertFalse(reader.hasReachedEndOfFile());
            reader.readInto(createBuffer());
        }

        reader.close();

        assertTrue(reader.hasReachedEndOfFile());

        // Verify that the content is the same
        assertEquals("Read less buffers than written.", numBuffers, returnedBuffers.size());

        // Start number after skipped buffers...
        currentNumber = (BUFFER_SIZE / 4) * toSkip;

        Buffer buffer;
        while ((buffer = returnedBuffers.poll()) != null) {
            currentNumber = verifyBufferFilledWithAscendingNumbers(buffer, currentNumber);
        }
    }

    // ------------------------------------------------------------------------

    private int getRandomNumberInRange(int min, int max) {
        return random.nextInt((max - min) + 1) + min;
    }

    private int getNextMultipleOf(int number, int multiple) {
        final int mod = number % multiple;

        if (mod == 0) {
            return number;
        }

        return number + multiple - mod;
    }

    private Buffer createBuffer() {
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE), BUFFER_RECYCLER);
    }

    static int fillBufferWithAscendingNumbers(Buffer buffer, int currentNumber, int size) {
        checkArgument(size % 4 == 0);

        MemorySegment segment = buffer.getMemorySegment();

        for (int i = 0; i < size; i += 4) {
            segment.putInt(i, currentNumber++);
        }
        buffer.setSize(size);

        return currentNumber;
    }

    static int verifyBufferFilledWithAscendingNumbers(Buffer buffer, int currentNumber) {
        MemorySegment segment = buffer.getMemorySegment();

        int size = buffer.getSize();

        for (int i = 0; i < size; i += 4) {
            if (segment.getInt(i) != currentNumber++) {
                throw new IllegalStateException("Read unexpected number from buffer.");
            }
        }

        return currentNumber;
    }
}
