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
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.io.network.partition.PartitionedFileWriteReadTest.createAndConfigIndexEntryBuffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SortMergeResultPartitionReadScheduler}. */
class SortMergeResultPartitionReadSchedulerTest {

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

    @BeforeEach
    void before(@TempDir Path basePath) throws Exception {
        Random random = new Random();
        random.nextBytes(dataBytes);
        partitionedFile =
                PartitionTestUtils.createPartitionedFile(
                        basePath.toString(),
                        numSubpartitions,
                        numBuffersPerSubpartition,
                        bufferSize,
                        dataBytes);
        dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        fileReader =
                new PartitionedFileReader(
                        partitionedFile,
                        0,
                        dataFileChannel,
                        indexFileChannel,
                        BufferReaderWriterUtil.allocatedHeaderBuffer(),
                        createAndConfigIndexEntryBuffer());
        bufferPool = new BatchShuffleReadBufferPool(totalBytes, bufferSize);
        executor = Executors.newFixedThreadPool(numThreads);
        readScheduler =
                new SortMergeResultPartitionReadScheduler(bufferPool, executor, new Object());
    }

    @AfterEach
    void after() throws Exception {
        dataFileChannel.close();
        indexFileChannel.close();
        partitionedFile.deleteQuietly();
        bufferPool.destroy();
        executor.shutdown();
    }

    @Test
    @Timeout(60)
    void testCreateSubpartitionReader() throws Exception {
        ManuallyTriggeredScheduledExecutorService ioExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        readScheduler =
                new SortMergeResultPartitionReadScheduler(bufferPool, ioExecutor, new Object());

        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.createSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        assertThat(readScheduler.isRunning()).isTrue();
        assertThat(readScheduler.getDataFileChannel().isOpen()).isTrue();
        assertThat(readScheduler.getIndexFileChannel().isOpen()).isTrue();
        assertThat(ioExecutor.numQueuedRunnables()).isEqualTo(1);

        int numBuffersRead = 0;
        while (numBuffersRead < numBuffersPerSubpartition) {
            ioExecutor.triggerAll();
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
    void testOnSubpartitionReaderError() throws Exception {
        SortMergeSubpartitionReader subpartitionReader =
                readScheduler.createSubpartitionReader(
                        new NoOpBufferAvailablityListener(), 0, partitionedFile);

        subpartitionReader.releaseAllResources();
        waitUntilReadFinish();
        assertAllResourcesReleased();
    }

    @Test
    void testReleaseWhileReading() throws Exception {
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

    @Test
    void testCreateSubpartitionReaderAfterReleased() throws Exception {
        bufferPool.initialize();
        readScheduler.release();
        assertThatThrownBy(
                        () ->
                                readScheduler.createSubpartitionReader(
                                        new NoOpBufferAvailablityListener(), 0, partitionedFile))
                .isInstanceOf(IllegalStateException.class);
        assertAllResourcesReleased();
    }

    @Test
    void testOnDataReadError() throws Exception {
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
    void testOnReadBufferRequestError() throws Exception {
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

    @Test
    @Timeout(60)
    void testNoDeadlockWhenReadAndReleaseBuffers() throws Exception {
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
    void testRequestBufferTimeout() throws Exception {
        Duration bufferRequestTimeout = Duration.ofSeconds(3);
        // avoid auto trigger reading.
        ManuallyTriggeredScheduledExecutorService executorService =
                new ManuallyTriggeredScheduledExecutorService();
        SortMergeResultPartitionReadScheduler readScheduler =
                new SortMergeResultPartitionReadScheduler(
                        bufferPool, executorService, this, bufferRequestTimeout);
        long startTimestamp = System.currentTimeMillis();
        readScheduler.createSubpartitionReader(
                new NoOpBufferAvailablityListener(), 0, partitionedFile);
        // request and use all buffers of buffer pool.
        readScheduler.run();

        assertThat(bufferPool.getAvailableBuffers()).isZero();
        assertThatThrownBy(readScheduler::allocateBuffers).isInstanceOf(TimeoutException.class);
        long requestDuration = System.currentTimeMillis() - startTimestamp;
        assertThat(requestDuration >= bufferRequestTimeout.toMillis()).isTrue();

        readScheduler.release();
    }

    @Test
    void testRequestTimeoutIsRefreshedAndSuccess() throws Exception {
        Duration bufferRequestTimeout = Duration.ofSeconds(3);
        FakeBatchShuffleReadBufferPool bufferPool =
                new FakeBatchShuffleReadBufferPool(bufferSize * 3, bufferSize);
        SortMergeResultPartitionReadScheduler readScheduler =
                new SortMergeResultPartitionReadScheduler(
                        bufferPool, executor, this, bufferRequestTimeout);

        long startTimestamp = System.currentTimeMillis();
        Queue<MemorySegment> allocatedBuffers = new ArrayDeque<>();

        assertThatCode(() -> allocatedBuffers.addAll(readScheduler.allocateBuffers()))
                .doesNotThrowAnyException();
        long requestDuration = System.currentTimeMillis() - startTimestamp;

        assertThat(allocatedBuffers).hasSize(3);
        assertThat(requestDuration).isGreaterThan(bufferRequestTimeout.toMillis() * 2);

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
        assertThat(readScheduler.getNumPendingReaders()).isZero();

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
