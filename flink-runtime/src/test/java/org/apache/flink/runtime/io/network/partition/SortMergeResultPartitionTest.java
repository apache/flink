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
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.Random;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link SortMergeResultPartition}. */
public class SortMergeResultPartitionTest {

    private static final BufferAvailabilityListener listener = new NoOpBufferAvailablityListener();

    private static final int bufferSize = 1024;

    private static final int totalBuffers = 1000;

    private FileChannelManager fileChannelManager;

    private NetworkBufferPool globalPool;

    @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void setUp() {
        fileChannelManager =
                new FileChannelManagerImpl(new String[] {tmpFolder.getRoot().getPath()}, "testing");
        globalPool = new NetworkBufferPool(totalBuffers, bufferSize);
    }

    @After
    public void shutdown() throws Exception {
        fileChannelManager.close();
        globalPool.destroy();
    }

    @Test
    public void testWriteAndRead() throws Exception {
        int numSubpartitions = 10;
        int numBuffers = 100;
        int numRecords = 1000;
        Random random = new Random();

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition =
                createSortMergedPartition(numSubpartitions, bufferPool);

        Queue<PartitionSortedBufferTest.DataAndType>[] dataWritten = new Queue[numSubpartitions];
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
            byte[] data = new byte[random.nextInt(2 * bufferSize) + 1];
            random.nextBytes(data);
            ByteBuffer record = ByteBuffer.wrap(data);
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

        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            ResultSubpartitionView view = partition.createSubpartitionView(subpartition, listener);
            while (view.isAvailable(Integer.MAX_VALUE)) {
                Buffer buffer = view.getNextBuffer().buffer();
                int numBytes = buffer.readableBytes();
                numBytesRead[subpartition] += numBytes;

                MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                segment.put(0, buffer.getNioBufferReadable(), numBytes);
                buffersRead[subpartition].add(
                        new NetworkBuffer(segment, (buf) -> {}, buffer.getDataType(), numBytes));
                buffer.recycleBuffer();
            }
            view.releaseAllResources();
        }

        PartitionSortedBufferTest.checkWriteReadResult(
                numSubpartitions, numBytesWritten, numBytesRead, dataWritten, buffersRead);
    }

    private void recordDataWritten(
            ByteBuffer record,
            Queue<PartitionSortedBufferTest.DataAndType>[] dataWritten,
            int subpartition,
            int[] numBytesWritten,
            Buffer.DataType dataType) {
        record.rewind();
        dataWritten[subpartition].add(new PartitionSortedBufferTest.DataAndType(record, dataType));
        numBytesWritten[subpartition] += record.remaining();
    }

    @Test
    public void testWriteLargeRecord() throws Exception {
        int numBuffers = 100;
        int numWriteBuffers = numBuffers / 2;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);

        byte[] dataWritten = new byte[bufferSize * numBuffers];
        Random random = new Random();
        random.nextBytes(dataWritten);
        ByteBuffer recordWritten = ByteBuffer.wrap(dataWritten);
        partition.emitRecord(recordWritten, 0);
        assertEquals(numWriteBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        partition.finish();
        partition.close();

        ResultSubpartitionView view = partition.createSubpartitionView(0, listener);
        ByteBuffer recordRead = ByteBuffer.allocate(bufferSize * numBuffers);
        while (view.isAvailable(Integer.MAX_VALUE)) {
            Buffer buffer = view.getNextBuffer().buffer();
            if (buffer.isBuffer()) {
                recordRead.put(buffer.getNioBufferReadable());
            }
            buffer.recycleBuffer();
        }
        view.releaseAllResources();

        recordWritten.rewind();
        recordRead.flip();
        assertEquals(recordWritten, recordRead);
    }

    @Test
    public void testFlush() throws Exception {
        int numBuffers = 10;
        int numWriteBuffers = numBuffers / 2;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertEquals(numWriteBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        partition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        partition.emitRecord(ByteBuffer.allocate(bufferSize), 1);
        assertEquals(3 + numWriteBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        partition.flush(0);
        assertEquals(numWriteBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        partition.emitRecord(ByteBuffer.allocate(bufferSize), 2);
        partition.emitRecord(ByteBuffer.allocate(bufferSize), 3);
        assertEquals(3 + numWriteBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        partition.flushAll();
        assertEquals(numWriteBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        assertNull(partition.getResultFile());
        partition.finish();
        assertEquals(3, partition.getResultFile().getNumRegions());

        partition.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testReleaseWhileWriting() throws Exception {
        int numBuffers = 10;
        int numWriteBuffers = numBuffers / 2;
        int numBuffersForSort = numBuffers - numWriteBuffers;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertEquals(numWriteBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 0);
        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 1);

        partition.emitRecord(ByteBuffer.allocate(bufferSize), 2);
        assertNull(partition.getResultFile());
        assertEquals(2, fileChannelManager.getPaths()[0].list().length);

        partition.release();
        try {
            partition.emitRecord(ByteBuffer.allocate(bufferSize * numBuffers), 2);
        } catch (IllegalStateException exception) {
            assertEquals(0, fileChannelManager.getPaths()[0].list().length);

            throw exception;
        }
    }

    @Test
    public void testReleaseWhileReading() throws Exception {
        int numBuffers = 10;
        int numWriteBuffers = numBuffers / 2;
        int numBuffersForSort = numBuffers - numWriteBuffers;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertEquals(numWriteBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 0);
        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 1);
        partition.finish();
        partition.close();

        assertEquals(2, partition.getResultFile().getNumRegions());
        assertEquals(2, fileChannelManager.getPaths()[0].list().length);

        ResultSubpartitionView view = partition.createSubpartitionView(0, listener);
        view.getNextBuffer().buffer().recycleBuffer();
        partition.release();

        assertEquals(2, partition.getResultFile().getNumRegions());
        assertEquals(2, fileChannelManager.getPaths()[0].list().length);

        while (view.isAvailable(Integer.MAX_VALUE)) {
            view.getNextBuffer().buffer().recycleBuffer();
        }
        view.releaseAllResources();

        assertNull(partition.getResultFile());
        assertEquals(0, fileChannelManager.getPaths()[0].list().length);
    }

    @Test
    public void testCloseReleasesAllBuffers() throws Exception {
        int numBuffers = 100;
        int numWriteBuffers = numBuffers / 2;
        int numBuffersForSort = numBuffers - numWriteBuffers;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
        assertEquals(numWriteBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        partition.emitRecord(ByteBuffer.allocate(bufferSize * (numBuffersForSort - 1)), 5);
        assertEquals(numBuffers, bufferPool.bestEffortGetNumOfUsedBuffers());

        partition.close();
        assertTrue(bufferPool.isDestroyed());
        assertEquals(totalBuffers, globalPool.getNumberOfAvailableMemorySegments());
    }

    @Test(expected = IllegalStateException.class)
    public void testReadUnfinishedPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(10, 10);
        try {
            SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
            partition.createSubpartitionView(0, listener);
        } finally {
            bufferPool.lazyDestroy();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testReadReleasedPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(10, 10);
        try {
            SortMergeResultPartition partition = createSortMergedPartition(10, bufferPool);
            partition.finish();
            partition.release();
            partition.createSubpartitionView(0, listener);
        } finally {
            bufferPool.lazyDestroy();
        }
    }

    private SortMergeResultPartition createSortMergedPartition(
            int numSubpartitions, BufferPool bufferPool) throws IOException {
        SortMergeResultPartition sortMergedResultPartition =
                new SortMergeResultPartition(
                        "SortMergedResultPartitionTest",
                        0,
                        new ResultPartitionID(),
                        ResultPartitionType.BLOCKING,
                        numSubpartitions,
                        numSubpartitions,
                        bufferSize,
                        new ResultPartitionManager(),
                        fileChannelManager.createChannel().getPath(),
                        null,
                        () -> bufferPool);
        sortMergedResultPartition.setup();
        return sortMergedResultPartition;
    }
}
