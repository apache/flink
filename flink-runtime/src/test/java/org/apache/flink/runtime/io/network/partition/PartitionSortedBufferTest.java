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
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link PartitionSortedBuffer}. */
public class PartitionSortedBufferTest {

    @Test
    public void testWriteAndReadSortBuffer() throws Exception {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        int bufferPoolSize = 1000;
        Random random = new Random(1111);

        // used to store data written to and read from sort buffer for correctness check
        Queue<DataAndType>[] dataWritten = new Queue[numSubpartitions];
        Queue<Buffer>[] buffersRead = new Queue[numSubpartitions];
        for (int i = 0; i < numSubpartitions; ++i) {
            dataWritten[i] = new ArrayDeque<>();
            buffersRead[i] = new ArrayDeque<>();
        }

        int[] numBytesWritten = new int[numSubpartitions];
        int[] numBytesRead = new int[numSubpartitions];
        Arrays.fill(numBytesWritten, 0);
        Arrays.fill(numBytesRead, 0);

        // fill the sort buffer with randomly generated data
        int totalBytesWritten = 0;
        SortBuffer sortBuffer =
                createSortBuffer(
                        bufferPoolSize,
                        bufferSize,
                        numSubpartitions,
                        getRandomSubpartitionOrder(numSubpartitions));
        while (true) {
            // record size may be larger than buffer size so a record may span multiple segments
            int recordSize = random.nextInt(bufferSize * 4 - 1) + 1;
            byte[] bytes = new byte[recordSize];

            // fill record with random value
            random.nextBytes(bytes);
            ByteBuffer record = ByteBuffer.wrap(bytes);

            // select a random subpartition to write
            int subpartition = random.nextInt(numSubpartitions);

            // select a random data type
            boolean isBuffer = random.nextBoolean();
            DataType dataType = isBuffer ? DataType.DATA_BUFFER : DataType.EVENT_BUFFER;
            if (!sortBuffer.append(record, subpartition, dataType)) {
                sortBuffer.finish();
                break;
            }
            record.rewind();
            dataWritten[subpartition].add(new DataAndType(record, dataType));
            numBytesWritten[subpartition] += recordSize;
            totalBytesWritten += recordSize;
        }

        // read all data from the sort buffer
        while (sortBuffer.hasRemaining()) {
            MemorySegment readBuffer = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
            SortBuffer.BufferWithChannel bufferAndChannel = sortBuffer.copyIntoSegment(readBuffer);
            int subpartition = bufferAndChannel.getChannelIndex();
            buffersRead[subpartition].add(bufferAndChannel.getBuffer());
            numBytesRead[subpartition] += bufferAndChannel.getBuffer().readableBytes();
        }

        assertEquals(totalBytesWritten, sortBuffer.numBytes());
        checkWriteReadResult(
                numSubpartitions, numBytesWritten, numBytesRead, dataWritten, buffersRead);
    }

    public static void checkWriteReadResult(
            int numSubpartitions,
            int[] numBytesWritten,
            int[] numBytesRead,
            Queue<DataAndType>[] dataWritten,
            Queue<Buffer>[] buffersRead) {
        for (int subpartitionIndex = 0; subpartitionIndex < numSubpartitions; ++subpartitionIndex) {
            assertEquals(numBytesWritten[subpartitionIndex], numBytesRead[subpartitionIndex]);

            List<DataAndType> eventsWritten = new ArrayList<>();
            List<Buffer> eventsRead = new ArrayList<>();

            ByteBuffer subpartitionDataWritten =
                    ByteBuffer.allocate(numBytesWritten[subpartitionIndex]);
            for (DataAndType dataAndType : dataWritten[subpartitionIndex]) {
                subpartitionDataWritten.put(dataAndType.data);
                dataAndType.data.rewind();
                if (dataAndType.dataType.isEvent()) {
                    eventsWritten.add(dataAndType);
                }
            }

            ByteBuffer subpartitionDataRead = ByteBuffer.allocate(numBytesRead[subpartitionIndex]);
            for (Buffer buffer : buffersRead[subpartitionIndex]) {
                subpartitionDataRead.put(buffer.getNioBufferReadable());
                if (!buffer.isBuffer()) {
                    eventsRead.add(buffer);
                }
            }

            subpartitionDataWritten.flip();
            subpartitionDataRead.flip();
            assertEquals(subpartitionDataWritten, subpartitionDataRead);

            assertEquals(eventsWritten.size(), eventsRead.size());
            for (int i = 0; i < eventsWritten.size(); ++i) {
                assertEquals(eventsWritten.get(i).dataType, eventsRead.get(i).getDataType());
                assertEquals(eventsWritten.get(i).data, eventsRead.get(i).getNioBufferReadable());
            }
        }
    }

    @Test
    public void testWriteReadWithEmptyChannel() throws Exception {
        int bufferPoolSize = 10;
        int bufferSize = 1024;
        int numSubpartitions = 5;

        ByteBuffer[] subpartitionRecords = {
            ByteBuffer.allocate(128),
            null,
            ByteBuffer.allocate(1536),
            null,
            ByteBuffer.allocate(1024)
        };

        SortBuffer sortBuffer = createSortBuffer(bufferPoolSize, bufferSize, numSubpartitions);
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            ByteBuffer record = subpartitionRecords[subpartition];
            if (record != null) {
                sortBuffer.append(record, subpartition, Buffer.DataType.DATA_BUFFER);
                record.rewind();
            }
        }
        sortBuffer.finish();

        checkReadResult(sortBuffer, subpartitionRecords[0], 0, bufferSize);

        ByteBuffer expected1 = subpartitionRecords[2].duplicate();
        expected1.limit(bufferSize);
        checkReadResult(sortBuffer, expected1.slice(), 2, bufferSize);

        ByteBuffer expected2 = subpartitionRecords[2].duplicate();
        expected2.position(bufferSize);
        checkReadResult(sortBuffer, expected2.slice(), 2, bufferSize);

        checkReadResult(sortBuffer, subpartitionRecords[4], 4, bufferSize);
    }

    private void checkReadResult(
            SortBuffer sortBuffer, ByteBuffer expectedBuffer, int expectedChannel, int bufferSize) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
        SortBuffer.BufferWithChannel bufferWithChannel = sortBuffer.copyIntoSegment(segment);
        assertEquals(expectedChannel, bufferWithChannel.getChannelIndex());
        assertEquals(expectedBuffer, bufferWithChannel.getBuffer().getNioBufferReadable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWriteEmptyData() throws Exception {
        int bufferSize = 1024;

        SortBuffer sortBuffer = createSortBuffer(1, bufferSize, 1);

        ByteBuffer record = ByteBuffer.allocate(1);
        record.position(1);

        sortBuffer.append(record, 0, Buffer.DataType.DATA_BUFFER);
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteFinishedSortBuffer() throws Exception {
        int bufferSize = 1024;

        SortBuffer sortBuffer = createSortBuffer(1, bufferSize, 1);
        sortBuffer.finish();

        sortBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteReleasedSortBuffer() throws Exception {
        int bufferSize = 1024;

        SortBuffer sortBuffer = createSortBuffer(1, bufferSize, 1);
        sortBuffer.release();

        sortBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);
    }

    @Test
    public void testWriteMoreDataThanCapacity() throws Exception {
        int bufferPoolSize = 10;
        int bufferSize = 1024;

        SortBuffer sortBuffer = createSortBuffer(bufferPoolSize, bufferSize, 1);

        for (int i = 1; i < bufferPoolSize; ++i) {
            appendAndCheckResult(sortBuffer, bufferSize, true, bufferSize * i, i, true);
        }

        // append should fail for insufficient capacity
        int numRecords = bufferPoolSize - 1;
        appendAndCheckResult(
                sortBuffer, bufferSize, false, bufferSize * numRecords, numRecords, true);
    }

    @Test
    public void testWriteLargeRecord() throws Exception {
        int bufferPoolSize = 10;
        int bufferSize = 1024;

        SortBuffer sortBuffer = createSortBuffer(bufferPoolSize, bufferSize, 1);
        // append should fail for insufficient capacity
        appendAndCheckResult(sortBuffer, bufferPoolSize * bufferSize, false, 0, 0, false);
    }

    private void appendAndCheckResult(
            SortBuffer sortBuffer,
            int recordSize,
            boolean isSuccessful,
            long numBytes,
            long numRecords,
            boolean hasRemaining)
            throws IOException {
        ByteBuffer largeRecord = ByteBuffer.allocate(recordSize);

        assertEquals(isSuccessful, sortBuffer.append(largeRecord, 0, Buffer.DataType.DATA_BUFFER));
        assertEquals(numBytes, sortBuffer.numBytes());
        assertEquals(numRecords, sortBuffer.numRecords());
        assertEquals(hasRemaining, sortBuffer.hasRemaining());
    }

    @Test(expected = IllegalStateException.class)
    public void testReadUnfinishedSortBuffer() throws Exception {
        int bufferSize = 1024;

        SortBuffer sortBuffer = createSortBuffer(1, bufferSize, 1);
        sortBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);

        assertTrue(sortBuffer.hasRemaining());
        sortBuffer.copyIntoSegment(MemorySegmentFactory.allocateUnpooledSegment(bufferSize));
    }

    @Test(expected = IllegalStateException.class)
    public void testReadReleasedSortBuffer() throws Exception {
        int bufferSize = 1024;

        SortBuffer sortBuffer = createSortBuffer(1, bufferSize, 1);
        sortBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);
        sortBuffer.finish();
        assertTrue(sortBuffer.hasRemaining());

        sortBuffer.release();
        assertFalse(sortBuffer.hasRemaining());

        sortBuffer.copyIntoSegment(MemorySegmentFactory.allocateUnpooledSegment(bufferSize));
    }

    @Test(expected = IllegalStateException.class)
    public void testReadEmptySortBuffer() throws Exception {
        int bufferSize = 1024;

        SortBuffer sortBuffer = createSortBuffer(1, bufferSize, 1);
        sortBuffer.finish();

        assertFalse(sortBuffer.hasRemaining());
        sortBuffer.copyIntoSegment(MemorySegmentFactory.allocateUnpooledSegment(bufferSize));
    }

    @Test
    public void testReleaseSortBuffer() throws Exception {
        int bufferPoolSize = 10;
        int bufferSize = 1024;
        int recordSize = (bufferPoolSize - 1) * bufferSize;

        NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, bufferSize);
        BufferPool bufferPool = globalPool.createBufferPool(bufferPoolSize, bufferPoolSize);

        SortBuffer sortBuffer =
                new PartitionSortedBuffer(new Object(), bufferPool, 1, bufferSize, null);
        sortBuffer.append(ByteBuffer.allocate(recordSize), 0, Buffer.DataType.DATA_BUFFER);

        assertEquals(bufferPoolSize, bufferPool.bestEffortGetNumOfUsedBuffers());
        assertTrue(sortBuffer.hasRemaining());
        assertEquals(1, sortBuffer.numRecords());
        assertEquals(recordSize, sortBuffer.numBytes());

        // should release all data and resources
        sortBuffer.release();
        assertEquals(0, bufferPool.bestEffortGetNumOfUsedBuffers());
        assertFalse(sortBuffer.hasRemaining());
        assertEquals(0, sortBuffer.numRecords());
        assertEquals(0, sortBuffer.numBytes());
    }

    private SortBuffer createSortBuffer(int bufferPoolSize, int bufferSize, int numSubpartitions)
            throws IOException {
        return createSortBuffer(bufferPoolSize, bufferSize, numSubpartitions, null);
    }

    private SortBuffer createSortBuffer(
            int bufferPoolSize, int bufferSize, int numSubpartitions, int[] customReadOrder)
            throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, bufferSize);
        BufferPool bufferPool = globalPool.createBufferPool(bufferPoolSize, bufferPoolSize);

        return new PartitionSortedBuffer(
                new Object(), bufferPool, numSubpartitions, bufferSize, customReadOrder);
    }

    public static int[] getRandomSubpartitionOrder(int numSubpartitions) {
        Random random = new Random(1111);
        int[] subpartitionReadOrder = new int[numSubpartitions];
        int shift = random.nextInt(numSubpartitions);
        for (int i = 0; i < numSubpartitions; ++i) {
            subpartitionReadOrder[i] = (i + shift) % numSubpartitions;
        }
        return subpartitionReadOrder;
    }

    /** Data written and its {@link Buffer.DataType}. */
    public static class DataAndType {
        private final ByteBuffer data;
        private final Buffer.DataType dataType;

        DataAndType(ByteBuffer data, Buffer.DataType dataType) {
            this.data = data;
            this.dataType = dataType;
        }
    }
}
