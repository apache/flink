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
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
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
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SortMergeResultPartitionReadScheduler}. */
public class SortMergeResultPartitionReadSchedulerTest extends TestLogger {

    private static final int bufferSize = 1024;

    private static final byte[] dataBytes = new byte[bufferSize];

    private static final int totalBytes = bufferSize * 2;

    private static final int numThreads = 4;

    private static final int numSubpartitions = 10;

    private static final int numBuffersPerSubpartition = 10;

    private PartitionedFile partitionedFile;

    private PartitionedFileReader fileReader;

    private FileChannel dataFileChannel;

    private FileChannel indexFileChannel;

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
        dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        fileReader =
                new PartitionedFileReader(partitionedFile, 0, dataFileChannel, indexFileChannel);
        bufferPool = new BatchShuffleReadBufferPool(totalBytes, bufferSize);
        executor = Executors.newFixedThreadPool(numThreads);
        readScheduler =
                new SortMergeResultPartitionReadScheduler(bufferPool, executor, new Object());
    }

    @After
    public void after() throws Exception {
        dataFileChannel.close();
        indexFileChannel.close();
        partitionedFile.deleteQuietly();
        bufferPool.destroy();
        executor.shutdown();
    }

    @Test
    public void testCreateSubpartitionReader() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.createSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        assertThat(readScheduler.isRunning()).isTrue();
        assertThat(readScheduler.getDataFileChannel().isOpen()).isTrue();
        assertThat(readScheduler.getIndexFileChannel().isOpen()).isTrue();

        int numBuffersRead = 0;
        while (numBuffersRead < numBuffersPerSubpartition) {
            ResultSubpartition.BufferAndBacklog bufferAndBacklog =
                    subpartitionReader.getNextBuffer();
            if (bufferAndBacklog != null) {
                int numBytes = bufferAndBacklog.buffer().readableBytes();
                MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                Buffer fullBuffer =
                        ((CompositeBuffer) bufferAndBacklog.buffer()).getFullBufferData(segment);
                assertThat(ByteBuffer.wrap(dataBytes)).isEqualTo(fullBuffer.getNioBufferReadable());
                fullBuffer.recycleBuffer();
                ++numBuffersRead;
            }
        }
    }

    @Test
    public void testOnSubpartitionReaderError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.createSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        subpartitionReader.releaseAllResources();
        waitUntilReadFinish();
        assertAllResourcesReleased();
    }

    @Test
    public void testReleaseWhileReading() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.createSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        Thread.sleep(1000);
        readScheduler.release();

        assertThat(subpartitionReader.getFailureCause()).isNotNull();
        assertThat(subpartitionReader.isReleased()).isTrue();
        assertThat(subpartitionReader.unsynchronizedGetNumberOfQueuedBuffers()).isEqualTo(0);
        assertThat(subpartitionReader.getAvailabilityAndBacklog(0).isAvailable()).isTrue();

        readScheduler.getReleaseFuture().get();
        assertAllResourcesReleased();
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateSubpartitionReaderAfterReleased() throws Exception {
        bufferPool.initialize();
        readScheduler.release();
        try {
            readScheduler.createSubpartitionReader(
                    new NoOpBufferAvailablityListener(), 0, partitionedFile);
        } finally {
            assertAllResourcesReleased();
        }
    }

    @Test
    public void testOnDataReadError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.createSubpartitionReader(
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
        assertThat(subpartitionReader.getFailureCause()).isNotNull();
        assertThat(subpartitionReader.getAvailabilityAndBacklog(0).isAvailable()).isTrue();
        assertAllResourcesReleased();
    }

    @Test
    public void testOnReadBufferRequestError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.createSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        bufferPool.destroy();
        waitUntilReadFinish();

        assertThat(subpartitionReader.isReleased()).isTrue();
        assertThat(subpartitionReader.getFailureCause()).isNotNull();
        assertThat(subpartitionReader.getAvailabilityAndBacklog(0).isAvailable()).isTrue();
        assertAllResourcesReleased();
    }

    @Test(timeout = 60000)
    public void testNoDeadlockWhenReadAndReleaseBuffers() throws Exception {
        bufferPool.initialize();
        SortMergeSubpartitionReader subpartitionReader =
                new SortMergeSubpartitionReader(new NoOpBufferAvailablityListener(), fileReader);
        Thread readAndReleaseThread =
                new Thread(
                        () -> {
                            Queue<MemorySegment> segments = new ArrayDeque<>();
                            segments.add(MemorySegmentFactory.allocateUnpooledSegment(bufferSize));
                            try {
                                assertThat(fileReader.hasRemaining()).isTrue();
                                subpartitionReader.readBuffers(segments, readScheduler);
                                subpartitionReader.releaseAllResources();
                                subpartitionReader.readBuffers(segments, readScheduler);
                            } catch (Exception ignore) {
                            }
                        });

        synchronized (this) {
            readAndReleaseThread.start();
            do {
                Thread.sleep(100);
            } while (!subpartitionReader.isReleased());
        }
        readAndReleaseThread.join();
    }

    @Test
    public void testRequestBufferTimeout() throws Exception {
        Duration bufferRequestTimeout = Duration.ofSeconds(3);
        List<MemorySegment> buffers = bufferPool.requestBuffers();
        SortMergeResultPartitionReadScheduler readScheduler =
                new SortMergeResultPartitionReadScheduler(
                        bufferPool, executor, this, bufferRequestTimeout);

        long startTimestamp = System.nanoTime();
        Assertions.assertThatThrownBy(readScheduler::allocateBuffers)
                .isInstanceOf(TimeoutException.class);
        long requestDuration = System.nanoTime() - startTimestamp;
        assertThat(requestDuration > bufferRequestTimeout.toNanos()).isTrue();

        bufferPool.recycle(buffers);
        readScheduler.release();
    }

    @Test
    public void testRequestTimeoutIsRefreshedAndSuccess() throws Exception {
        Duration bufferRequestTimeout = Duration.ofSeconds(3);
        FakeBatchShuffleReadBufferPool bufferPool =
                new FakeBatchShuffleReadBufferPool(bufferSize * 3, bufferSize);
        SortMergeResultPartitionReadScheduler readScheduler =
                new SortMergeResultPartitionReadScheduler(
                        bufferPool, executor, this, bufferRequestTimeout);
        SortMergeSubpartitionReader subpartitionReader =
                new SortMergeSubpartitionReader(new NoOpBufferAvailablityListener(), fileReader);

        long startTimestamp = System.nanoTime();
        Queue<MemorySegment> allocatedBuffers = readScheduler.allocateBuffers();
        long requestDuration = System.nanoTime() - startTimestamp;

        assertThat(allocatedBuffers.size()).isEqualTo(3);
        assertThat(requestDuration > bufferRequestTimeout.toNanos() * 2).isTrue();
        assertThat(subpartitionReader.getFailureCause()).isNull();

        bufferPool.recycle(allocatedBuffers);
        bufferPool.destroy();
        readScheduler.release();
    }

    private static class FakeBatchShuffleReadBufferPool extends BatchShuffleReadBufferPool {
        private final Queue<MemorySegment> requestedBuffers;

        FakeBatchShuffleReadBufferPool(long totalBytes, int bufferSize) throws Exception {
            super(totalBytes, bufferSize);
            this.requestedBuffers = new LinkedList<>(requestBuffers());
        }

        @Override
        public long getLastBufferOperationTimestamp() {
            recycle(requestedBuffers.poll());
            return super.getLastBufferOperationTimestamp();
        }

        @Override
        public void destroy() {
            recycle(requestedBuffers);
            requestedBuffers.clear();
            super.destroy();
        }
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private void assertAllResourcesReleased() {
        assertThat(readScheduler.getDataFileChannel()).isNull();
        assertThat(readScheduler.getIndexFileChannel()).isNull();
        assertThat(readScheduler.isRunning()).isFalse();
        assertThat(readScheduler.getNumPendingReaders()).isEqualTo(0);

        if (!bufferPool.isDestroyed()) {
            assertThat(bufferPool.getNumTotalBuffers()).isEqualTo(bufferPool.getAvailableBuffers());
        }
    }

    private void waitUntilReadFinish() throws Exception {
        while (readScheduler.isRunning()) {
            Thread.sleep(100);
        }
    }
}
