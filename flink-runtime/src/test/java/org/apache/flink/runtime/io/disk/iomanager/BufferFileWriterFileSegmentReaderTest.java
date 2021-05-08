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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.util.event.NotificationListener;
import org.apache.flink.util.IOUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.runtime.io.disk.iomanager.BufferFileWriterReaderTest.fillBufferWithAscendingNumbers;
import static org.apache.flink.runtime.io.disk.iomanager.BufferFileWriterReaderTest.verifyBufferFilledWithAscendingNumbers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BufferFileWriterFileSegmentReaderTest {

    private static final int BUFFER_SIZE = 32 * 1024;

    private static final BufferRecycler BUFFER_RECYCLER = FreeingBufferRecycler.INSTANCE;

    private static final Random random = new Random();

    private static final IOManager ioManager = new IOManagerAsync();

    private BufferFileWriter writer;

    private AsynchronousBufferFileSegmentReader reader;

    private LinkedBlockingQueue<FileSegment> returnedFileSegments = new LinkedBlockingQueue<>();

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
                    (AsynchronousBufferFileSegmentReader)
                            ioManager.createBufferFileSegmentReader(
                                    channel, new QueuingCallback<>(returnedFileSegments));
        } catch (IOException e) {
            tearDownWriterAndReader();

            fail("Failed to setup writer and reader.");
        }
    }

    @After
    public void tearDownWriterAndReader() {
        if (writer != null) {
            if (!writer.isClosed()) {
                IOUtils.closeQuietly(() -> writer.close());
            }
            writer.deleteChannel();
        }

        if (reader != null) {
            if (!reader.isClosed()) {
                IOUtils.closeQuietly(() -> reader.close());
            }
            reader.deleteChannel();
        }

        returnedFileSegments.clear();
    }

    @Test
    public void testWriteRead() throws IOException, InterruptedException {
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
            reader.read();
        }

        // Wait for all requests to be finished
        final CountDownLatch sync = new CountDownLatch(1);
        final NotificationListener listener =
                new NotificationListener() {
                    @Override
                    public void onNotification() {
                        sync.countDown();
                    }
                };

        if (reader.registerAllRequestsProcessedListener(listener)) {
            sync.await();
        }

        assertTrue(reader.hasReachedEndOfFile());

        // Verify that the content is the same
        assertEquals("Read less buffers than written.", numBuffers, returnedFileSegments.size());

        currentNumber = 0;
        FileSegment fileSegment;

        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

        while ((fileSegment = returnedFileSegments.poll()) != null) {
            buffer.position(0);
            buffer.limit(fileSegment.getLength());

            fileSegment.getFileChannel().read(buffer, fileSegment.getPosition());

            Buffer buffer1 =
                    new NetworkBuffer(MemorySegmentFactory.wrap(buffer.array()), BUFFER_RECYCLER);
            buffer1.setSize(fileSegment.getLength());
            currentNumber = verifyBufferFilledWithAscendingNumbers(buffer1, currentNumber);
        }

        reader.close();
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
}
