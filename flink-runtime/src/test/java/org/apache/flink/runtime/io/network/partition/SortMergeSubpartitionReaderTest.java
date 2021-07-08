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
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link SortMergeSubpartitionReader}. */
public class SortMergeSubpartitionReaderTest extends TestLogger {

    private static final int bufferSize = 1024;

    private static final byte[] dataBytes = new byte[bufferSize];

    private static final int numSubpartitions = 10;

    private static final int numBuffersPerSubpartition = 10;

    private PartitionedFile partitionedFile;

    private FileChannel dataFileChannel;

    private FileChannel indexFileChannel;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Before
    public void before() throws Exception {
        Random random = new Random();
        random.nextBytes(dataBytes);
        partitionedFile =
                PartitionTestUtils.createPartitionedFile(
                        temporaryFolder.newFile().getAbsolutePath(),
                        numSubpartitions,
                        numBuffersPerSubpartition,
                        bufferSize,
                        dataBytes);
        dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
    }

    @After
    public void after() {
        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);
        partitionedFile.deleteQuietly();
    }

    @Test
    public void testReadBuffers() throws Exception {
        CountingAvailabilityListener listener = new CountingAvailabilityListener();
        SortMergeSubpartitionReader subpartitionReader =
                createSortMergeSubpartitionReader(listener);

        assertEquals(0, listener.numNotifications);
        assertEquals(0, subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers());

        Queue<MemorySegment> segments = createsMemorySegments(2);
        subpartitionReader.readBuffers(segments, FreeingBufferRecycler.INSTANCE);

        assertEquals(1, listener.numNotifications);
        assertEquals(2, subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers());
        assertEquals(0, segments.size());

        segments = createsMemorySegments(2);
        subpartitionReader.readBuffers(segments, FreeingBufferRecycler.INSTANCE);

        assertEquals(1, listener.numNotifications);
        assertEquals(4, subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers());
        assertEquals(0, segments.size());

        while (subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers() > 0) {
            checkNotNull(subpartitionReader.getNextBuffer()).buffer().recycleBuffer();
        }

        segments = createsMemorySegments(numBuffersPerSubpartition);
        subpartitionReader.readBuffers(segments, FreeingBufferRecycler.INSTANCE);

        assertEquals(2, listener.numNotifications);
        assertEquals(
                numBuffersPerSubpartition - 4,
                subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers());
        assertEquals(4, segments.size());
    }

    @Test
    public void testPollBuffers() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                createSortMergeSubpartitionReader(new CountingAvailabilityListener());

        assertNull(subpartitionReader.getNextBuffer());
        assertFalse(subpartitionReader.getAvailabilityAndBacklog(Integer.MAX_VALUE).isAvailable());

        Queue<MemorySegment> segments = createsMemorySegments(numBuffersPerSubpartition);
        subpartitionReader.readBuffers(segments, FreeingBufferRecycler.INSTANCE);

        for (int i = numBuffersPerSubpartition - 1; i >= 0; --i) {
            assertTrue(subpartitionReader.getAvailabilityAndBacklog(i).isAvailable());
            ResultSubpartition.BufferAndBacklog bufferAndBacklog =
                    checkNotNull(subpartitionReader.getNextBuffer());
            assertEquals(
                    ByteBuffer.wrap(dataBytes), bufferAndBacklog.buffer().getNioBufferReadable());
            assertEquals(bufferAndBacklog.buffersInBacklog(), i == 0 ? 0 : i - 1);
            Buffer.DataType dataType =
                    i == 0
                            ? Buffer.DataType.NONE
                            : (i > 1 ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER);
            assertEquals(dataType, bufferAndBacklog.getNextDataType());
        }
    }

    @Test
    public void testFail() throws Exception {
        int numSegments = 5;
        Queue<MemorySegment> segments = createsMemorySegments(numSegments);

        try {
            CountingAvailabilityListener listener = new CountingAvailabilityListener();
            SortMergeSubpartitionReader subpartitionReader =
                    createSortMergeSubpartitionReader(listener);

            subpartitionReader.readBuffers(segments, segments::add);
            assertEquals(1, listener.numNotifications);
            assertEquals(5, subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers());

            subpartitionReader.fail(new RuntimeException("Test exception."));
            assertTrue(subpartitionReader.getReleaseFuture().isDone());
            assertEquals(0, subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers());
            assertTrue(subpartitionReader.getAvailabilityAndBacklog(0).isAvailable());
            assertTrue(subpartitionReader.isReleased());

            assertEquals(2, listener.numNotifications);
            assertNotNull(subpartitionReader.getFailureCause());
        } finally {
            assertEquals(numSegments, segments.size());
        }
    }

    @Test
    public void testReleaseAllResources() throws Exception {
        int numSegments = 5;
        Queue<MemorySegment> segments = createsMemorySegments(numSegments);

        try {
            CountingAvailabilityListener listener = new CountingAvailabilityListener();
            SortMergeSubpartitionReader subpartitionReader =
                    createSortMergeSubpartitionReader(listener);

            subpartitionReader.readBuffers(segments, segments::add);
            assertEquals(1, listener.numNotifications);
            assertEquals(5, subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers());

            subpartitionReader.releaseAllResources();
            assertTrue(subpartitionReader.getReleaseFuture().isDone());
            assertEquals(0, subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers());
            assertTrue(subpartitionReader.getAvailabilityAndBacklog(0).isAvailable());
            assertTrue(subpartitionReader.isReleased());

            assertEquals(1, listener.numNotifications);
            assertNull(subpartitionReader.getFailureCause());
        } finally {
            assertEquals(numSegments, segments.size());
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testReadBuffersAfterReleased() throws Exception {
        int numSegments = 5;
        Queue<MemorySegment> segments = createsMemorySegments(numSegments);

        try {
            SortMergeSubpartitionReader subpartitionReader =
                    createSortMergeSubpartitionReader(new CountingAvailabilityListener());

            subpartitionReader.readBuffers(segments, segments::add);
            subpartitionReader.releaseAllResources();
            subpartitionReader.readBuffers(segments, segments::add);
        } finally {
            assertEquals(numSegments, segments.size());
        }
    }

    @Test
    public void testPollBuffersAfterReleased() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                createSortMergeSubpartitionReader(new CountingAvailabilityListener());

        Queue<MemorySegment> segments = createsMemorySegments(numBuffersPerSubpartition);
        subpartitionReader.readBuffers(segments, FreeingBufferRecycler.INSTANCE);

        assertTrue(subpartitionReader.getAvailabilityAndBacklog(Integer.MAX_VALUE).isAvailable());
        subpartitionReader.releaseAllResources();
        assertNull(subpartitionReader.getNextBuffer());
    }

    private SortMergeSubpartitionReader createSortMergeSubpartitionReader(
            BufferAvailabilityListener listener) throws Exception {
        PartitionedFileReader fileReader =
                new PartitionedFileReader(partitionedFile, 0, dataFileChannel, indexFileChannel);
        assertTrue(fileReader.hasRemaining());
        return new SortMergeSubpartitionReader(listener, fileReader);
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private static Queue<MemorySegment> createsMemorySegments(int numSegments) {
        Queue<MemorySegment> segments = new ArrayDeque<>();
        for (int i = 0; i < numSegments; ++i) {
            segments.add(MemorySegmentFactory.allocateUnpooledSegment(bufferSize));
        }
        return segments;
    }
}
