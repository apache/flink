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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HsResultPartitionReadScheduler}. */
@ExtendWith(TestLoggerExtension.class)
class HsResultPartitionReadSchedulerTest {
    private static final int BUFFER_SIZE = 1024;

    private static final int NUM_SUBPARTITIONS = 10;

    private static final int BUFFER_POOL_SIZE = 2;

    private final byte[] dataBytes = new byte[BUFFER_SIZE];

    private ManuallyTriggeredScheduledExecutor ioExecutor;

    private BatchShuffleReadBufferPool bufferPool;

    private FileChannel dataFileChannel;

    private Path dataFilePath;

    private HsResultPartitionReadScheduler readScheduler;

    private TestingSubpartitionViewInternalOperation subpartitionViewOperation;

    private TestingHsSubpartitionFileReader.Factory factory;

    @BeforeEach
    void before(@TempDir Path tempDir) throws IOException {
        Random random = new Random();
        random.nextBytes(dataBytes);
        bufferPool = new BatchShuffleReadBufferPool(BUFFER_POOL_SIZE * BUFFER_SIZE, BUFFER_SIZE);
        ioExecutor = new ManuallyTriggeredScheduledExecutor();
        dataFilePath = Files.createFile(tempDir.resolve(".data"));
        dataFileChannel = openFileChannel(dataFilePath);
        factory = new TestingHsSubpartitionFileReader.Factory();
        readScheduler =
                new HsResultPartitionReadScheduler(
                        bufferPool,
                        ioExecutor,
                        new HsFileDataIndexImpl(NUM_SUBPARTITIONS),
                        dataFilePath,
                        factory,
                        HybridShuffleConfiguration.builder(
                                        NUM_SUBPARTITIONS, bufferPool.getNumBuffersPerRequest())
                                .build());
        subpartitionViewOperation = new TestingSubpartitionViewInternalOperation();
    }

    @AfterEach
    void after() throws Exception {
        bufferPool.destroy();
        if (dataFileChannel != null) {
            dataFileChannel.close();
        }
    }

    // ----------------------- test run and register ---------------------------------------

    @Test
    void testRegisterReaderTriggerRun() throws Exception {
        TestingHsSubpartitionFileReader reader = new TestingHsSubpartitionFileReader();
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> readBuffers.addAll(requestedBuffers));
        factory.allReaders.add(reader);

        assertThat(reader.readBuffers).isEmpty();

        readScheduler.registerNewSubpartition(0, subpartitionViewOperation);

        ioExecutor.trigger();

        assertThat(reader.readBuffers).hasSize(BUFFER_POOL_SIZE);
    }

    @Test
    void testBufferReleasedTriggerRun() throws Exception {
        TestingHsSubpartitionFileReader reader = new TestingHsSubpartitionFileReader();
        reader.setReadBuffersConsumer(
                (requestedBuffer, readBuffers) -> {
                    while (!requestedBuffer.isEmpty()) {
                        readBuffers.add(requestedBuffer.poll());
                    }
                });

        factory.allReaders.add(reader);

        readScheduler.registerNewSubpartition(0, subpartitionViewOperation);

        ioExecutor.trigger();

        assertThat(reader.readBuffers).hasSize(BUFFER_POOL_SIZE);
        assertThat(bufferPool.getAvailableBuffers()).isZero();

        readScheduler.recycle(reader.readBuffers.poll());
        readScheduler.recycle(reader.readBuffers.poll());

        // recycle buffer will push new runnable to ioExecutor.
        ioExecutor.trigger();
        assertThat(reader.readBuffers).hasSize(BUFFER_POOL_SIZE);
    }

    /** Test all not used buffers will be released after run method finish. */
    @Test
    void testRunReleaseUnusedBuffers() throws Exception {
        TestingHsSubpartitionFileReader reader = new TestingHsSubpartitionFileReader();

        CompletableFuture<Void> prepareForSchedulingFinished = new CompletableFuture<>();
        reader.setPrepareForSchedulingRunnable(() -> prepareForSchedulingFinished.complete(null));
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    assertThat(prepareForSchedulingFinished).isCompleted();
                    assertThat(requestedBuffers).hasSize(BUFFER_POOL_SIZE);
                    assertThat(bufferPool.getAvailableBuffers()).isEqualTo(0);
                    // read one buffer, return another buffer to scheduler.
                    readBuffers.add(requestedBuffers.poll());
                });
        factory.allReaders.add(reader);

        readScheduler.registerNewSubpartition(0, subpartitionViewOperation);

        ioExecutor.trigger();

        // not used buffer should be recycled.
        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(1);
    }

    /** Test scheduler will schedule readers in order. */
    @Test
    void testScheduleReadersOrdered() throws Exception {
        TestingHsSubpartitionFileReader reader1 = new TestingHsSubpartitionFileReader();
        TestingHsSubpartitionFileReader reader2 = new TestingHsSubpartitionFileReader();

        CompletableFuture<Void> readBuffersFinished1 = new CompletableFuture<>();
        CompletableFuture<Void> readBuffersFinished2 = new CompletableFuture<>();
        reader1.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    assertThat(readBuffersFinished2).isNotDone();
                    readBuffersFinished1.complete(null);
                });
        reader2.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    assertThat(readBuffersFinished1).isDone();
                    readBuffersFinished2.complete(null);
                });

        reader1.setPriority(1);
        reader2.setPriority(2);

        factory.allReaders.add(reader1);
        factory.allReaders.add(reader2);

        readScheduler.registerNewSubpartition(0, subpartitionViewOperation);
        readScheduler.registerNewSubpartition(1, subpartitionViewOperation);

        // trigger run.
        ioExecutor.trigger();

        assertThat(readBuffersFinished2).isCompleted();
    }

    @Test
    void testRunRequestBufferTimeout() throws Exception {
        Duration bufferRequestTimeout = Duration.ofSeconds(3);

        // request all buffer first.
        bufferPool.requestBuffers();
        assertThat(bufferPool.getAvailableBuffers()).isZero();

        readScheduler =
                new HsResultPartitionReadScheduler(
                        bufferPool,
                        ioExecutor,
                        new HsFileDataIndexImpl(NUM_SUBPARTITIONS),
                        dataFilePath,
                        factory,
                        HybridShuffleConfiguration.builder(
                                        NUM_SUBPARTITIONS, bufferPool.getNumBuffersPerRequest())
                                .setBufferRequestTimeout(bufferRequestTimeout)
                                .build());

        TestingHsSubpartitionFileReader reader = new TestingHsSubpartitionFileReader();
        CompletableFuture<Void> prepareForSchedulingFinished = new CompletableFuture<>();
        CompletableFuture<Throwable> cause = new CompletableFuture<>();
        reader.setPrepareForSchedulingRunnable(() -> prepareForSchedulingFinished.complete(null));
        reader.setFailConsumer((cause::complete));
        factory.allReaders.add(reader);

        readScheduler.registerNewSubpartition(0, subpartitionViewOperation);

        ioExecutor.trigger();

        assertThat(prepareForSchedulingFinished).isCompleted();
        assertThat(cause).isCompleted();
        assertThat(cause.get())
                .isInstanceOf(TimeoutException.class)
                .hasMessageContaining("Buffer request timeout");
    }

    /**
     * When {@link HsSubpartitionFileReader#readBuffers(Queue, BufferRecycler)} throw IOException,
     * subpartition reader should fail.
     */
    @Test
    void testRunReadBuffersThrowException() throws Exception {
        TestingHsSubpartitionFileReader reader = new TestingHsSubpartitionFileReader();
        CompletableFuture<Throwable> cause = new CompletableFuture<>();
        reader.setFailConsumer((cause::complete));
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    throw new IOException("expected exception.");
                });
        factory.allReaders.add(reader);

        readScheduler.registerNewSubpartition(0, subpartitionViewOperation);

        ioExecutor.trigger();

        assertThat(cause).isCompleted();
        assertThat(cause.get())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expected exception.");
    }

    // ----------------------- test release ---------------------------------------

    /** Test scheduler release when reader is reading buffers. */
    @Test
    @Timeout(10)
    void testReleasedWhenReading() throws Exception {
        TestingHsSubpartitionFileReader reader = new TestingHsSubpartitionFileReader();

        CompletableFuture<Throwable> cause = new CompletableFuture<>();
        reader.setFailConsumer((cause::complete));
        CompletableFuture<Void> readBufferStart = new CompletableFuture<>();
        CompletableFuture<Void> schedulerReleasedFinish = new CompletableFuture<>();
        reader.setReadBuffersConsumer(
                (requestedBuffers, readBuffers) -> {
                    try {
                        readBufferStart.complete(null);
                        schedulerReleasedFinish.get();
                    } catch (Exception e) {
                        // re-throw all exception to IOException caught by read scheduler.
                        throw new IOException(e);
                    }
                });
        factory.allReaders.add(reader);

        CheckedThread releaseThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        readBufferStart.get();
                        readScheduler.release();
                        schedulerReleasedFinish.complete(null);
                    }
                };
        releaseThread.start();

        readScheduler.registerNewSubpartition(0, subpartitionViewOperation);

        ioExecutor.trigger();

        releaseThread.sync();

        assertThat(cause).isCompleted();
        assertThat(cause.get())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Result partition has been already released.");
    }

    /** Test scheduler was released, but receive new subpartition reader registration. */
    @Test
    void testRegisterSubpartitionReaderAfterSchedulerReleased() {
        TestingHsSubpartitionFileReader reader = new TestingHsSubpartitionFileReader();
        factory.allReaders.add(reader);

        readScheduler.release();
        assertThatThrownBy(
                        () -> {
                            readScheduler.registerNewSubpartition(0, subpartitionViewOperation);
                            ioExecutor.trigger();
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("HsResultPartitionReadScheduler is already released.");
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private static class TestingHsSubpartitionFileReader implements HsSubpartitionFileReader {
        private Runnable prepareForSchedulingRunnable = () -> {};

        private BiConsumerWithException<Queue<MemorySegment>, Queue<MemorySegment>, IOException>
                readBuffersConsumer = (ignore1, ignore2) -> {};

        private Consumer<Throwable> failConsumer = (ignore) -> {};

        private final Queue<MemorySegment> readBuffers;

        private int priority;

        private TestingHsSubpartitionFileReader() {
            this.readBuffers = new ArrayDeque<>();
        }

        @Override
        public void prepareForScheduling() {
            prepareForSchedulingRunnable.run();
        }

        @Override
        public void readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler)
                throws IOException {
            readBuffersConsumer.accept(buffers, readBuffers);
        }

        @Override
        public void fail(Throwable failureCause) {
            failConsumer.accept(failureCause);
        }

        @Override
        public int compareTo(HsSubpartitionFileReader that) {
            checkArgument(that instanceof TestingHsSubpartitionFileReader);
            return Integer.compare(priority, ((TestingHsSubpartitionFileReader) that).priority);
        }

        public void setPriority(int priority) {
            this.priority = priority;
        }

        public void setPrepareForSchedulingRunnable(Runnable prepareForSchedulingRunnable) {
            this.prepareForSchedulingRunnable = prepareForSchedulingRunnable;
        }

        public void setReadBuffersConsumer(
                BiConsumerWithException<Queue<MemorySegment>, Queue<MemorySegment>, IOException>
                        readBuffersConsumer) {
            this.readBuffersConsumer = readBuffersConsumer;
        }

        public void setFailConsumer(Consumer<Throwable> failConsumer) {
            this.failConsumer = failConsumer;
        }

        /** Factory for {@link TestingHsSubpartitionFileReader}. */
        private static class Factory implements HsSubpartitionFileReader.Factory {
            private final Queue<HsSubpartitionFileReader> allReaders = new ArrayDeque<>();

            @Override
            public HsSubpartitionFileReader createFileReader(
                    int subpartitionId,
                    FileChannel dataFileChannel,
                    HsSubpartitionViewInternalOperations operation,
                    HsFileDataIndex dataIndex,
                    int maxBuffersReadAhead) {
                return checkNotNull(allReaders.poll());
            }
        }
    }
}
