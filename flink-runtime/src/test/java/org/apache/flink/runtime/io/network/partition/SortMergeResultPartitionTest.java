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
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SortMergeResultPartition}. */
@ExtendWith(ParameterizedTestExtension.class)
public class SortMergeResultPartitionTest {

    private static final int bufferSize = 1024;

    private static final int totalBuffers = 1000;

    private static final int totalBytes = 32 * 1024 * 1024;

    private static final int numThreads = 4;

    @Parameter public boolean useHashDataBuffer;

    private final TestBufferAvailabilityListener listener = new TestBufferAvailabilityListener();

    private FileChannelManager fileChannelManager;

    private NetworkBufferPool globalPool;

    private BatchShuffleReadBufferPool readBufferPool;

    private ExecutorService readIOExecutor;

    @TempDir private Path tmpFolder;

    @BeforeEach
    void setUp() throws IOException {
        fileChannelManager =
                new FileChannelManagerImpl(
                        new String[] {TempDirUtils.newFolder(tmpFolder).toString()}, "testing");
        globalPool = new NetworkBufferPool(totalBuffers, bufferSize);
        readBufferPool = new BatchShuffleReadBufferPool(totalBytes, bufferSize);
        readIOExecutor = Executors.newFixedThreadPool(numThreads);
    }

    @AfterEach
    void shutdown() throws Exception {
        fileChannelManager.close();
        globalPool.destroy();
        readBufferPool.destroy();
        readIOExecutor.shutdown();
    }

    @Parameters(name = "useHashDataBuffer={0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @TestTemplate
    void testWriteAndRead() throws Exception {
        int numBuffers = useHashDataBuffer ? 100 : 15;
        int numSubpartitions = 10;
        int numRecords = 1000;
        Random random = new Random();

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition =
                createSortMergedPartition(numSubpartitions, bufferPool);

        Queue<DataBufferTest.DataAndType>[] dataWritten = new Queue[numSubpartitions];
        Queue<Buffer>[] buffersRead = new Queue[numSubpartitions];
        for (int i = 0; i < numSubpartitions; ++i) {
            dataWritten[i] = new ArrayDeque<>();
            buffersRead[i] = new ArrayDeque<>();
        }

        int[] numBytesWritten = new int[numSubpartitions];
        int[] numBytesRead = new int[numSubpartitions];
        Arrays.fill(numBytesWritten, 0);
        Arrays.fill(numBytesRead, 0);

        for (int i = 0; i < numRecords; ++i) {
            ByteBuffer record = generateRandomData(random.nextInt(2 * bufferSize) + 1, random);
            boolean isBroadCast = random.nextBoolean();

            if (isBroadCast) {
                partition.broadcastRecord(record);
                for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                    recordDataWritten(
                            record,
                            dataWritten,
                            subpartition,
                            numBytesWritten,
                            DataType.DATA_BUFFER);
                }
            } else {
                int subpartition = random.nextInt(numSubpartitions);
                partition.emitRecord(record, subpartition);
                recordDataWritten(
                        record, dataWritten, subpartition, numBytesWritten, DataType.DATA_BUFFER);
            }
        }

        partition.finish();
        partition.close();
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            ByteBuffer record = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
            recordDataWritten(
                    record, dataWritten, subpartition, numBytesWritten, DataType.EVENT_BUFFER);
        }

        ResultSubpartitionView[] views = createSubpartitionViews(partition, numSubpartitions);
        readData(
                views,
                bufferWithChannel -> {
                    Buffer buffer = bufferWithChannel.getBuffer();
                    int subpartition = bufferWithChannel.getChannelIndex();

                    int numBytes = buffer.readableBytes();
                    numBytesRead[subpartition] += numBytes;

                    MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                    Buffer fullBuffer =
                            ((CompositeBuffer) buffer)
                                    .getFullBufferData(
                                            MemorySegmentFactory.allocateUnpooledSegment(numBytes));
                    segment.put(0, fullBuffer.getNioBufferReadable(), fullBuffer.readableBytes());
                    buffersRead[subpartition].add(
                            new NetworkBuffer(
                                    segment,
                                    ignore -> {},
                                    fullBuffer.getDataType(),
                                    fullBuffer.isCompressed(),
                                    fullBuffer.readableBytes()));
                    fullBuffer.recycleBuffer();
                });
        DataBufferTest.checkWriteReadResult(
                numSubpartitions, numBytesWritten, numBytesRead, dataWritten, buffersRead);
    }

    private void recordDataWritten(
            ByteBuffer record,
            Queue<DataBufferTest.DataAndType>[] dataWritten,
            int subpartition,
            int[] numBytesWritten,
            Buffer.DataType dataType) {
        record.rewind();
        dataWritten[subpartition].add(new DataBufferTest.DataAndType(record, dataType));
        numBytesWritten[subpartition] += record.remaining();
    }

    private ByteBuffer generateRandomData(int dataSize, Random random) {
        byte[] dataWritten = new byte[dataSize];
        random.nextBytes(dataWritten);
        return ByteBuffer.wrap(dataWritten);
    }

    private long readData(
            ResultSubpartitionView[] views, Consumer<BufferWithChannel> bufferProcessor)
            throws Exception {
        int dataSize = 0;
        int numEndOfPartitionEvents = 0;

        while (numEndOfPartitionEvents < views.length) {
            listener.waitForData();
            for (int subpartition = 0; subpartition < views.length; ++subpartition) {
                ResultSubpartitionView view = views[subpartition];
                ResultSubpartition.BufferAndBacklog bufferAndBacklog = view.getNextBuffer();
                while (bufferAndBacklog != null) {
                    Buffer buffer = bufferAndBacklog.buffer();
                    bufferProcessor.accept(new BufferWithChannel(buffer, subpartition));
                    dataSize += buffer.readableBytes();

                    if (!buffer.isBuffer()) {
                        ++numEndOfPartitionEvents;
                        assertThat(view.getAvailabilityAndBacklog(Integer.MAX_VALUE).isAvailable())
                                .isFalse();
                        view.releaseAllResources();
                    }
                    bufferAndBacklog = view.getNextBuffer();
                }
            }
        }
        return dataSize;
    }

    private ResultSubpartitionView[] createSubpartitionViews(
            SortMergeResultPartition partition, int numSubpartitions) throws Exception {
        ResultSubpartitionView[] views = new ResultSubpartitionView[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            views[subpartition] = partition.createSubpartitionView(subpartition, listener);
        }
        return views;
    }

    @TestTemplate
    void testWriteLargeRecord() throws Exception {
        int numBuffers = useHashDataBuffer ? 100 : 15;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);

        ByteBuffer recordWritten = generateRandomData(bufferSize * numBuffers, new Random());
        partition.emitRecord(recordWritten, 0);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers())
                .isEqualTo(useHashDataBuffer ? numBuffers : 0);

        partition.finish();
        partition.close();

        ResultSubpartitionView view = partition.createSubpartitionView(0, listener);
        ByteBuffer recordRead = ByteBuffer.allocate(bufferSize * numBuffers);
        readData(
                new ResultSubpartitionView[] {view},
                bufferWithChannel -> {
                    Buffer buffer = bufferWithChannel.getBuffer();
                    int numBytes = buffer.readableBytes();

                    MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                    Buffer fullBuffer = ((CompositeBuffer) buffer).getFullBufferData(segment);
                    if (fullBuffer.isBuffer()) {
                        ByteBuffer byteBuffer =
                                ByteBuffer.allocate(fullBuffer.readableBytes())
                                        .put(fullBuffer.getNioBufferReadable());
                        recordRead.put((ByteBuffer) byteBuffer.flip());
                    }
                    fullBuffer.recycleBuffer();
                });
        recordWritten.rewind();
        recordRead.flip();
        assertThat(recordRead).isEqualTo(recordWritten);
    }

    @TestTemplate
    void testDataBroadcast() throws Exception {
        int numSubpartitions = 10;
        int numBuffers = useHashDataBuffer ? 100 : 15;
        int numRecords = 10000;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition =
                createSortMergedPartition(numSubpartitions, bufferPool);

        for (int i = 0; i < numRecords; ++i) {
            ByteBuffer record = generateRandomData(bufferSize, new Random());
            partition.broadcastRecord(record);
        }
        partition.finish();
        partition.close();

        int eventSize = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE).remaining();
        long dataSize = numSubpartitions * numRecords * bufferSize + numSubpartitions * eventSize;
        assertThat(partition.getResultFile()).isNotNull();
        assertThat(checkNotNull(fileChannelManager.getPaths()[0].list()).length).isEqualTo(2);
        for (File file : checkNotNull(fileChannelManager.getPaths()[0].listFiles())) {
            if (file.getName().endsWith(PartitionedFile.DATA_FILE_SUFFIX)) {
                assertThat(file.length()).isLessThan(numSubpartitions * numRecords * bufferSize);
            }
        }

        ResultSubpartitionView[] views = createSubpartitionViews(partition, numSubpartitions);
        long dataRead =
                readData(
                        views,
                        bufferWithChannel -> {
                            bufferWithChannel.getBuffer().recycleBuffer();
                        });
        assertThat(dataRead).isEqualTo(dataSize);
    }

    @TestTemplate
    void testReleaseWhileWriting() throws Exception {
        int numBuffers = useHashDataBuffer ? 100 : 15;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numBuffers);

        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffers - 1)), 0);
        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffers - 1)), 1);

        partition.emitRecord(ByteBuffer.allocate(bufferSize), 2);
        assertThat(partition.getResultFile()).isNull();
        assertThat(fileChannelManager.getPaths()[0].list().length).isEqualTo(2);

        partition.release();

        assertThatThrownBy(
                        () -> partition.emitRecord(ByteBuffer.allocate(bufferSize * numBuffers), 2))
                .isInstanceOf(IllegalStateException.class);
        assertThat(fileChannelManager.getPaths()[0].list().length).isEqualTo(0);
    }

    @TestTemplate
    void testRelease() throws Exception {
        int numBuffers = useHashDataBuffer ? 100 : 15;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numBuffers);

        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffers - 1)), 0);
        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffers - 1)), 1);
        partition.finish();
        partition.close();

        assertThat(partition.getResultFile().getNumRegions()).isEqualTo(3);
        assertThat(checkNotNull(fileChannelManager.getPaths()[0].list()).length).isEqualTo(2);

        ResultSubpartitionView view = partition.createSubpartitionView(0, listener);
        partition.release();

        while (!view.isReleased() && partition.getResultFile() != null) {
            ResultSubpartition.BufferAndBacklog bufferAndBacklog = view.getNextBuffer();
            if (bufferAndBacklog != null) {
                bufferAndBacklog.buffer().recycleBuffer();
            }
        }

        // wait util partition file is released
        while (partition.getResultFile() != null) {
            Thread.sleep(100);
        }
        assertThat(checkNotNull(fileChannelManager.getPaths()[0].list()).length).isEqualTo(0);
    }

    @TestTemplate
    void testCloseReleasesAllBuffers() throws Exception {
        int numBuffers = useHashDataBuffer ? 100 : 15;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numBuffers);

        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffers - 1)), 5);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers())
                .isEqualTo(useHashDataBuffer ? numBuffers : 0);

        partition.close();
        assertThat(bufferPool.isDestroyed()).isTrue();
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(totalBuffers);
    }

    @TestTemplate
    void testReadUnfinishedPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(10, 10);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertThatThrownBy(() -> partition.createSubpartitionView(0, listener))
                .isInstanceOf(IllegalStateException.class);
        bufferPool.lazyDestroy();
    }

    @TestTemplate
    void testReadReleasedPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(10, 10);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        partition.finish();
        partition.release();

        assertThatThrownBy(() -> partition.createSubpartitionView(0, listener))
                .isInstanceOf(IllegalStateException.class);
        bufferPool.lazyDestroy();
    }

    @TestTemplate
    void testNumBytesProducedCounterForUnicast() throws IOException {
        testResultPartitionBytesCounter(false);
    }

    @TestTemplate
    void testNumBytesProducedCounterForBroadcast() throws IOException {
        testResultPartitionBytesCounter(true);
    }

    @TestTemplate
    void testNetworkBufferReservation() throws IOException {
        int numBuffers = 10;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, 2 * numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(1, bufferPool);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numBuffers);

        partition.finish();
        partition.close();
    }

    @TestTemplate
    void testNoDeadlockOnSpecificConsumptionOrder() throws Exception {
        // see https://issues.apache.org/jira/browse/FLINK-31386 for more information
        int numNetworkBuffers = 2 * BatchShuffleReadBufferPool.NUM_BYTES_PER_REQUEST / bufferSize;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(numNetworkBuffers, bufferSize);
        BatchShuffleReadBufferPool readBufferPool =
                new BatchShuffleReadBufferPool(
                        BatchShuffleReadBufferPool.NUM_BYTES_PER_REQUEST, bufferSize);

        BufferPool bufferPool =
                networkBufferPool.createBufferPool(numNetworkBuffers, numNetworkBuffers);
        SortMergeResultPartition partition =
                createSortMergedPartition(1, bufferPool, readBufferPool);
        for (int i = 0; i < numNetworkBuffers; ++i) {
            partition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        }
        partition.finish();
        partition.close();

        CountDownLatch condition1 = new CountDownLatch(1);
        CountDownLatch condition2 = new CountDownLatch(1);

        Runnable task1 =
                () -> {
                    try {
                        ResultSubpartitionView view = partition.createSubpartitionView(0, listener);
                        BufferPool bufferPool1 =
                                networkBufferPool.createBufferPool(
                                        numNetworkBuffers / 2, numNetworkBuffers);
                        SortMergeResultPartition partition1 =
                                createSortMergedPartition(1, bufferPool1);
                        readAndEmitData(view, partition1);

                        condition1.countDown();
                        condition2.await();
                        readAndEmitAllData(view, partition1);
                    } catch (Exception ignored) {
                    }
                };
        Thread consumer1 = new Thread(task1);
        consumer1.start();

        Runnable task2 =
                () -> {
                    try {
                        condition1.await();
                        BufferPool bufferPool2 =
                                networkBufferPool.createBufferPool(
                                        numNetworkBuffers / 2, numNetworkBuffers);
                        condition2.countDown();

                        SortMergeResultPartition partition2 =
                                createSortMergedPartition(1, bufferPool2);
                        ResultSubpartitionView view = partition.createSubpartitionView(0, listener);
                        readAndEmitAllData(view, partition2);
                    } catch (Exception ignored) {
                    }
                };
        Thread consumer2 = new Thread(task2);
        consumer2.start();

        consumer1.join();
        consumer2.join();
    }

    private boolean readAndEmitData(ResultSubpartitionView view, SortMergeResultPartition partition)
            throws Exception {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
        ResultSubpartition.BufferAndBacklog buffer;
        do {
            buffer = view.getNextBuffer();
            if (buffer != null) {
                Buffer data = ((CompositeBuffer) buffer.buffer()).getFullBufferData(segment);
                partition.emitRecord(data.getNioBufferReadable(), 0);
                if (!data.isRecycled()) {
                    data.recycleBuffer();
                }
                return buffer.buffer().isBuffer();
            }
        } while (true);
    }

    private void readAndEmitAllData(ResultSubpartitionView view, SortMergeResultPartition partition)
            throws Exception {
        while (readAndEmitData(view, partition)) {}
        partition.finish();
        partition.close();
    }

    private void testResultPartitionBytesCounter(boolean isBroadcast) throws IOException {
        int numBuffers = useHashDataBuffer ? 100 : 15;
        int numSubpartitions = 2;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition =
                createSortMergedPartition(numSubpartitions, bufferPool);

        if (isBroadcast) {
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            partition.finish();

            long[] subpartitionBytes =
                    partition.resultPartitionBytes.createSnapshot().getSubpartitionBytes();
            assertThat(subpartitionBytes)
                    .containsExactly((long) bufferSize + 4, (long) bufferSize + 4);

            assertThat(partition.numBytesOut.getCount())
                    .isEqualTo(numSubpartitions * (bufferSize + 4));
        } else {
            partition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
            partition.emitRecord(ByteBuffer.allocate(2 * bufferSize), 1);
            partition.finish();

            long[] subpartitionBytes =
                    partition.resultPartitionBytes.createSnapshot().getSubpartitionBytes();
            assertThat(subpartitionBytes)
                    .containsExactly((long) bufferSize + 4, (long) 2 * bufferSize + 4);

            assertThat(partition.numBytesOut.getCount())
                    .isEqualTo(3 * bufferSize + numSubpartitions * 4);
        }
    }

    private SortMergeResultPartition createSortMergedPartition(
            int numSubpartitions, BufferPool bufferPool) throws IOException {
        return createSortMergedPartition(numSubpartitions, bufferPool, readBufferPool);
    }

    private SortMergeResultPartition createSortMergedPartition(
            int numSubpartitions, BufferPool bufferPool, BatchShuffleReadBufferPool readBufferPool)
            throws IOException {
        SortMergeResultPartition sortMergedResultPartition =
                new SortMergeResultPartition(
                        "SortMergedResultPartitionTest",
                        0,
                        new ResultPartitionID(),
                        ResultPartitionType.BLOCKING,
                        numSubpartitions,
                        numSubpartitions,
                        readBufferPool,
                        readIOExecutor,
                        new ResultPartitionManager(),
                        fileChannelManager.createChannel().getPath(),
                        null,
                        () -> bufferPool);
        sortMergedResultPartition.setup();
        return sortMergedResultPartition;
    }

    private static final class TestBufferAvailabilityListener
            implements BufferAvailabilityListener {

        private int numNotifications;

        @Override
        public synchronized void notifyDataAvailable() {
            if (numNotifications == 0) {
                notifyAll();
            }
            ++numNotifications;
        }

        public synchronized void waitForData() throws InterruptedException {
            if (numNotifications == 0) {
                wait();
            }
            numNotifications = 0;
        }
    }
}
