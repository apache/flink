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

import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link SortMergeResultPartitionReadScheduler}. */
public class SortMergeResultPartitionReadSchedulerTest extends TestLogger {

    private static final int bufferSize = 1024;

    private static final byte[] dataBytes = new byte[bufferSize];

    private static final int totalBytes = bufferSize;

    private static final int numThreads = 4;

    private static final int numSubpartitions = 10;

    private static final int numBuffersPerSubpartition = 10;

    private PartitionedFile partitionedFile;

    private BatchShuffleReadBufferPool bufferPool;

    private ExecutorService executor;

    private SortMergeResultPartitionReadScheduler readScheduler;

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
        bufferPool = new BatchShuffleReadBufferPool(totalBytes, bufferSize);
        executor = Executors.newFixedThreadPool(numThreads);
        readScheduler = new SortMergeResultPartitionReadScheduler(bufferPool, executor, this);
    }

    @After
    public void after() {
        partitionedFile.deleteQuietly();
        bufferPool.destroy();
        executor.shutdown();
    }

    @Test
    public void testCreateSubpartitionReader() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        assertTrue(readScheduler.isRunning());
        assertTrue(readScheduler.getDataFileChannel().isOpen());
        assertTrue(readScheduler.getIndexFileChannel().isOpen());

        int numBuffersRead = 0;
        while (numBuffersRead < numBuffersPerSubpartition) {
            ResultSubpartition.BufferAndBacklog bufferAndBacklog =
                    subpartitionReader.getNextBuffer();
            if (bufferAndBacklog != null) {
                Buffer buffer = bufferAndBacklog.buffer();
                assertEquals(ByteBuffer.wrap(dataBytes), buffer.getNioBufferReadable());
                buffer.recycleBuffer();
                ++numBuffersRead;
            }
        }
    }

    @Test
    public void testOnSubpartitionReaderError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        subpartitionReader.releaseAllResources();
        waitUntilReadFinish();
        assertAllResourcesReleased();
    }

    @Test
    public void testReleaseWhileReading() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        Thread.sleep(1000);
        readScheduler.release();

        assertNotNull(subpartitionReader.getFailureCause());
        assertTrue(subpartitionReader.isReleased());
        assertEquals(0, subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers());
        assertTrue(subpartitionReader.isAvailable(0));

        readScheduler.getReleaseFuture().get();
        assertAllResourcesReleased();
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateSubpartitionReaderAfterReleased() throws Exception {
        readScheduler.release();
        try {
            readScheduler.crateSubpartitionReader(
                    new NoOpBufferAvailablityListener(), 0, partitionedFile);
        } finally {
            assertAllResourcesReleased();
        }
    }

    @Test
    public void testOnDataReadError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        // close file channel to trigger data read exception
        readScheduler.getDataFileChannel().close();

        while (!subpartitionReader.isReleased()) {
            ResultSubpartition.BufferAndBacklog bufferAndBacklog =
                    subpartitionReader.getNextBuffer();
            if (bufferAndBacklog != null) {
                bufferAndBacklog.buffer().recycleBuffer();
            }
        }

        waitUntilReadFinish();
        assertNotNull(subpartitionReader.getFailureCause());
        assertTrue(subpartitionReader.isAvailable(0));
        assertAllResourcesReleased();
    }

    @Test
    public void testOnReadBufferRequestError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.crateSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        bufferPool.destroy();
        waitUntilReadFinish();

        assertTrue(subpartitionReader.isReleased());
        assertNotNull(subpartitionReader.getFailureCause());
        assertTrue(subpartitionReader.isAvailable(0));
        assertAllResourcesReleased();
    }

    private void assertAllResourcesReleased() {
        assertNull(readScheduler.getDataFileChannel());
        assertNull(readScheduler.getIndexFileChannel());
        assertFalse(readScheduler.isRunning());
        assertEquals(0, readScheduler.getNumPendingReaders());

        if (!bufferPool.isDestroyed()) {
            assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());
        }
    }

    private void waitUntilReadFinish() throws Exception {
        while (readScheduler.isRunning()) {
            Thread.sleep(100);
        }
    }
}
