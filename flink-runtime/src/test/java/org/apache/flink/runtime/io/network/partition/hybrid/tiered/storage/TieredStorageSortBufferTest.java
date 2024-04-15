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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferWithSubpartition;
import org.apache.flink.runtime.io.network.partition.SortBuffer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TieredStorageSortBuffer}. */
class TieredStorageSortBufferTest {

    private static final int BUFFER_SIZE_BYTES = 1024;

    @Test
    void testWriteAndReadDataBuffer() throws Exception {
        int numSubpartitions = 10;
        int bufferPoolSize = 512;
        Random random = new Random(1234);

        // Used to store writing data and check the data correctness
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

        // Fill the sort buffer with randomly generated data
        TieredStorageSortBuffer sortBuffer = createDataBuffer(bufferPoolSize, numSubpartitions);
        int numDataBuffers = 5;
        while (numDataBuffers > 0) {
            // Record size may be larger than buffer size so a record may span multiple segments
            int recordSize = random.nextInt(BUFFER_SIZE_BYTES * 4 - 1) + 1;
            byte[] bytes = new byte[recordSize];

            // Fill record with random value
            random.nextBytes(bytes);
            ByteBuffer record = ByteBuffer.wrap(bytes);

            // Select a random subpartition to writeRecord
            int subpartition = random.nextInt(numSubpartitions);

            // Select a random data type
            boolean isBuffer = random.nextBoolean();
            Buffer.DataType dataType =
                    isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
            boolean writeSuccess = sortBuffer.append(record, subpartition, dataType);

            record.flip();
            if (record.hasRemaining()) {
                dataWritten[subpartition].add(new DataAndType(record, dataType));
                numBytesWritten[subpartition] += record.remaining();
            }

            if (writeSuccess) {
                continue;
            }
            sortBuffer.finish();
            --numDataBuffers;

            while (sortBuffer.hasRemaining()) {
                BufferWithSubpartition buffer = copyIntoSegment(sortBuffer);
                if (buffer == null) {
                    break;
                }
                addBufferRead(buffer, buffersRead, numBytesRead);
            }
            sortBuffer = createDataBuffer(bufferPoolSize, numSubpartitions);
        }

        // Read all data from the sort buffer
        if (sortBuffer.hasRemaining()) {
            sortBuffer.finish();
            while (sortBuffer.hasRemaining()) {
                addBufferRead(copyIntoSegment(sortBuffer), buffersRead, numBytesRead);
            }
        }

        checkWriteReadResult(
                numSubpartitions, numBytesWritten, numBytesRead, dataWritten, buffersRead);
    }

    @Test
    void testBufferIsRecycledWhenSortBufferIsEmpty() throws Exception {
        int numSubpartitions = 10;
        int bufferPoolSize = 512;
        int numBuffersForSort = 20;

        NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, BUFFER_SIZE_BYTES);
        BufferPool bufferPool =
                globalPool.createBufferPool(bufferPoolSize, bufferPoolSize, bufferPoolSize);

        LinkedList<MemorySegment> segments = new LinkedList<>();
        for (int i = 0; i < numBuffersForSort; ++i) {
            segments.add(bufferPool.requestMemorySegmentBlocking());
        }
        TieredStorageSortBuffer sortBuffer =
                new TieredStorageSortBuffer(
                        segments,
                        bufferPool,
                        numSubpartitions,
                        BUFFER_SIZE_BYTES,
                        numBuffersForSort,
                        true);
        MemorySegment memorySegment = segments.poll();
        sortBuffer.finish();
        assertThat(sortBuffer.getNextBuffer(memorySegment)).isNull();
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numBuffersForSort);
    }

    @Test
    void testBufferIsRecycledWhenGetEvent() throws Exception {
        int numSubpartitions = 10;
        int bufferPoolSize = 512;
        int bufferSizeBytes = 1024;
        int numBuffersForSort = 20;
        int subpartitionId = 0;
        Random random = new Random(1234);

        NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, bufferSizeBytes);
        BufferPool bufferPool =
                globalPool.createBufferPool(bufferPoolSize, bufferPoolSize, bufferPoolSize);

        LinkedList<MemorySegment> segments = new LinkedList<>();
        for (int i = 0; i < numBuffersForSort; ++i) {
            segments.add(bufferPool.requestMemorySegmentBlocking());
        }
        TieredStorageSortBuffer sortBuffer =
                new TieredStorageSortBuffer(
                        segments,
                        bufferPool,
                        numSubpartitions,
                        bufferSizeBytes,
                        numBuffersForSort,
                        true);

        byte[] bytes = new byte[1];
        random.nextBytes(bytes);
        ByteBuffer dataRecord = ByteBuffer.wrap(bytes);
        sortBuffer.append(dataRecord, subpartitionId, Buffer.DataType.DATA_BUFFER);
        ByteBuffer eventRecord = ByteBuffer.wrap(bytes);
        sortBuffer.append(eventRecord, subpartitionId, Buffer.DataType.EVENT_BUFFER);
        sortBuffer.finish();

        MemorySegment memorySegment = bufferPool.requestMemorySegmentBlocking();
        BufferWithSubpartition bufferWithSubpartition = sortBuffer.getNextBuffer(memorySegment);
        assertThat(bufferWithSubpartition.getBuffer().isBuffer()).isTrue();
        assertThat(bufferWithSubpartition.getSubpartitionIndex()).isEqualTo(subpartitionId);
        bufferWithSubpartition.getBuffer().recycleBuffer();
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numBuffersForSort);

        bufferWithSubpartition = sortBuffer.getNextBuffer(memorySegment);
        assertThat(bufferWithSubpartition.getBuffer().isBuffer()).isFalse();
        assertThat(bufferWithSubpartition.getSubpartitionIndex()).isEqualTo(subpartitionId);
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numBuffersForSort);
    }

    @Test
    void testReturnFreeSegments() throws IOException, InterruptedException {
        final int bufferPoolSize = 512;
        final int numBuffersForSort = 20;
        NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, BUFFER_SIZE_BYTES);
        BufferPool bufferPool =
                globalPool.createBufferPool(bufferPoolSize, bufferPoolSize, bufferPoolSize);

        LinkedList<MemorySegment> segments = new LinkedList<>();
        for (int i = 0; i < numBuffersForSort; ++i) {
            segments.add(bufferPool.requestMemorySegmentBlocking());
        }
        TieredStorageSortBuffer sortBuffer =
                new TieredStorageSortBuffer(
                        segments, bufferPool, 1, BUFFER_SIZE_BYTES, numBuffersForSort, true);

        for (int i = 0; i < 5; i++) {
            byte[] bytes = new byte[BUFFER_SIZE_BYTES];
            ByteBuffer record = ByteBuffer.wrap(bytes);
            sortBuffer.append(record, 0, Buffer.DataType.DATA_BUFFER);
        }
        assertThat(sortBuffer.returnFreeSegments(10)).isTrue();
        assertThat(sortBuffer.returnFreeSegments(10)).isFalse();

        sortBuffer.finish();
    }

    private static BufferWithSubpartition copyIntoSegment(SortBuffer dataBuffer) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE_BYTES);
        return dataBuffer.getNextBuffer(segment);
    }

    private static void addBufferRead(
            BufferWithSubpartition bufferWithSubpartition,
            Queue<Buffer>[] buffersRead,
            int[] numBytesRead) {
        int subpartition = bufferWithSubpartition.getSubpartitionIndex();
        Buffer buffer = bufferWithSubpartition.getBuffer();
        buffersRead[subpartition].add(
                new NetworkBuffer(
                        buffer.getMemorySegment(),
                        MemorySegment::free,
                        buffer.getDataType(),
                        buffer.getSize()));
        numBytesRead[subpartition] += buffer.getSize();
    }

    private static void checkWriteReadResult(
            int numSubpartitions,
            int[] numBytesWritten,
            int[] numBytesRead,
            Queue<DataAndType>[] dataWritten,
            Queue<Buffer>[] buffersRead) {
        for (int subpartitionIndex = 0; subpartitionIndex < numSubpartitions; ++subpartitionIndex) {
            assertThat(numBytesWritten[subpartitionIndex])
                    .isEqualTo(numBytesRead[subpartitionIndex]);

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
            assertThat(subpartitionDataWritten).isEqualTo(subpartitionDataRead);

            assertThat(eventsWritten.size()).isEqualTo(eventsRead.size());
            for (int i = 0; i < eventsWritten.size(); ++i) {
                assertThat(eventsWritten.get(i).dataType)
                        .isEqualTo(eventsRead.get(i).getDataType());
                assertThat(eventsWritten.get(i).data)
                        .isEqualTo(eventsRead.get(i).getNioBufferReadable());
            }
        }
    }

    private static TieredStorageSortBuffer createDataBuffer(
            int bufferPoolSize, int numSubpartitions) throws Exception {
        NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, BUFFER_SIZE_BYTES);
        BufferPool bufferPool =
                globalPool.createBufferPool(bufferPoolSize, bufferPoolSize, bufferPoolSize);

        LinkedList<MemorySegment> segments = new LinkedList<>();
        for (int i = 0; i < bufferPoolSize; ++i) {
            segments.add(bufferPool.requestMemorySegmentBlocking());
        }
        return new TieredStorageSortBuffer(
                segments, bufferPool, numSubpartitions, BUFFER_SIZE_BYTES, bufferPoolSize, true);
    }

    /** Data buffer with its {@link Buffer.DataType}. */
    public static class DataAndType {
        private final ByteBuffer data;
        private final Buffer.DataType dataType;

        DataAndType(ByteBuffer data, Buffer.DataType dataType) {
            this.data = data;
            this.dataType = dataType;
        }
    }
}
