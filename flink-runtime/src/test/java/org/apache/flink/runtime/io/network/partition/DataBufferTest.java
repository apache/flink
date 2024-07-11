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
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SortBasedDataBuffer} and {@link HashBasedDataBuffer}. */
@ExtendWith(ParameterizedTestExtension.class)
class DataBufferTest {

    private final boolean useHashBuffer;

    @Parameters(name = "UseHashBuffer = {0}")
    private static List<Boolean> parameters() {
        return Arrays.asList(true, false);
    }

    public DataBufferTest(boolean useHashBuffer) {
        this.useHashBuffer = useHashBuffer;
    }

    @TestTemplate
    void testWriteAndReadDataBuffer() throws Exception {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        int bufferPoolSize = 512;
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
        int[] subpartitionReadOrder = getRandomSubpartitionOrder(numSubpartitions);
        DataBuffer dataBuffer =
                createDataBuffer(
                        bufferPoolSize, bufferSize, numSubpartitions, subpartitionReadOrder);
        int numDataBuffers = 5;
        while (numDataBuffers > 0) {
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
            boolean isFull = dataBuffer.append(record, subpartition, dataType);

            record.flip();
            if (record.hasRemaining()) {
                dataWritten[subpartition].add(new DataAndType(record, dataType));
                numBytesWritten[subpartition] += record.remaining();
                totalBytesWritten += record.remaining();
            }

            if (!isFull) {
                continue;
            }
            dataBuffer.finish();
            --numDataBuffers;

            while (dataBuffer.hasRemaining()) {
                BufferWithSubpartition buffer = copyIntoSegment(bufferSize, dataBuffer);
                if (buffer == null) {
                    break;
                }
                addBufferRead(buffer, buffersRead, numBytesRead);
            }
            dataBuffer =
                    createDataBuffer(
                            bufferPoolSize, bufferSize, numSubpartitions, subpartitionReadOrder);
        }

        // read all data from the sort buffer
        if (dataBuffer.hasRemaining()) {
            assertThat(dataBuffer).isInstanceOf(HashBasedDataBuffer.class);
            dataBuffer.finish();
            while (dataBuffer.hasRemaining()) {
                addBufferRead(copyIntoSegment(bufferSize, dataBuffer), buffersRead, numBytesRead);
            }
        }

        assertThat(dataBuffer.numTotalBytes()).isZero();
        checkWriteReadResult(
                numSubpartitions, numBytesWritten, numBytesRead, dataWritten, buffersRead);
    }

    private BufferWithSubpartition copyIntoSegment(int bufferSize, DataBuffer dataBuffer) {
        if (useHashBuffer) {
            BufferWithSubpartition buffer = dataBuffer.getNextBuffer(null);
            if (buffer == null || !buffer.getBuffer().isBuffer()) {
                return buffer;
            }

            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
            int numBytes = buffer.getBuffer().readableBytes();
            segment.put(0, buffer.getBuffer().getNioBufferReadable(), numBytes);
            buffer.getBuffer().recycleBuffer();
            return new BufferWithSubpartition(
                    new NetworkBuffer(segment, MemorySegment::free, DataType.DATA_BUFFER, numBytes),
                    buffer.getSubpartitionIndex());
        } else {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
            return dataBuffer.getNextBuffer(segment);
        }
    }

    private void addBufferRead(
            BufferWithSubpartition buffer, Queue<Buffer>[] buffersRead, int[] numBytesRead) {
        int subpartition = buffer.getSubpartitionIndex();
        buffersRead[subpartition].add(buffer.getBuffer());
        numBytesRead[subpartition] += buffer.getBuffer().readableBytes();
    }

    public static void checkWriteReadResult(
            int numSubpartitions,
            int[] numBytesWritten,
            int[] numBytesRead,
            Queue<DataAndType>[] dataWritten,
            Queue<Buffer>[] buffersRead) {
        for (int subpartitionIndex = 0; subpartitionIndex < numSubpartitions; ++subpartitionIndex) {
            assertThat(numBytesRead[subpartitionIndex])
                    .isEqualTo(numBytesWritten[subpartitionIndex]);

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
            assertThat(subpartitionDataRead).isEqualTo(subpartitionDataWritten);

            assertThat(eventsRead).hasSameSizeAs(eventsWritten);
            for (int i = 0; i < eventsWritten.size(); ++i) {
                assertThat(eventsRead.get(i).getDataType())
                        .isEqualTo(eventsWritten.get(i).dataType);
                assertThat(eventsRead.get(i).getNioBufferReadable())
                        .isEqualTo(eventsWritten.get(i).data);
            }
        }
    }

    @TestTemplate
    public void testWriteReadWithEmptySubpartition() throws Exception {
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

        DataBuffer dataBuffer = createDataBuffer(bufferPoolSize, bufferSize, numSubpartitions);
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            ByteBuffer record = subpartitionRecords[subpartition];
            if (record != null) {
                dataBuffer.append(record, subpartition, Buffer.DataType.DATA_BUFFER);
                record.rewind();
            }
        }
        dataBuffer.finish();

        checkReadResult(dataBuffer, subpartitionRecords[0], 0, bufferSize);

        ByteBuffer expected1 = subpartitionRecords[2].duplicate();
        expected1.limit(bufferSize);
        checkReadResult(dataBuffer, expected1.slice(), 2, bufferSize);

        ByteBuffer expected2 = subpartitionRecords[2].duplicate();
        expected2.position(bufferSize);
        checkReadResult(dataBuffer, expected2.slice(), 2, bufferSize);

        checkReadResult(dataBuffer, subpartitionRecords[4], 4, bufferSize);
    }

    private void checkReadResult(
            DataBuffer dataBuffer,
            ByteBuffer expectedBuffer,
            int expectedSubpartition,
            int bufferSize) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
        BufferWithSubpartition bufferWithSubpartition = dataBuffer.getNextBuffer(segment);
        assertThat(bufferWithSubpartition.getSubpartitionIndex()).isEqualTo(expectedSubpartition);
        assertThat(bufferWithSubpartition.getBuffer().getNioBufferReadable())
                .isEqualTo(expectedBuffer);
    }

    @TestTemplate
    void testWriteEmptyData() throws Exception {
        int bufferSize = 1024;

        DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);

        ByteBuffer record = ByteBuffer.allocate(1);
        record.position(1);

        assertThatThrownBy(() -> dataBuffer.append(record, 0, Buffer.DataType.DATA_BUFFER))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @TestTemplate
    void testWriteFinishedDataBuffer() throws Exception {
        int bufferSize = 1024;

        DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
        dataBuffer.finish();

        assertThatThrownBy(
                        () ->
                                dataBuffer.append(
                                        ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER))
                .isInstanceOf(IllegalStateException.class);
    }

    @TestTemplate
    void testWriteReleasedDataBuffer() throws Exception {
        int bufferSize = 1024;

        DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
        dataBuffer.release();

        assertThatThrownBy(
                        () ->
                                dataBuffer.append(
                                        ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER))
                .isInstanceOf(IllegalStateException.class);
    }

    @TestTemplate
    void testWriteMoreDataThanCapacity() throws Exception {
        int bufferPoolSize = 10;
        int bufferSize = 1024;

        DataBuffer dataBuffer = createDataBuffer(bufferPoolSize, bufferSize, 1);

        for (int i = 1; i < bufferPoolSize; ++i) {
            appendAndCheckResult(dataBuffer, bufferSize, false, bufferSize * i, i, true);
        }

        // append should fail for insufficient capacity
        int numRecords = bufferPoolSize - 1;
        long numBytes = bufferSize * numRecords;
        appendAndCheckResult(dataBuffer, bufferSize + 1, true, numBytes, numRecords, true);
    }

    @TestTemplate
    void testWriteLargeRecord() throws Exception {
        int bufferPoolSize = 10;
        int bufferSize = 1024;

        DataBuffer dataBuffer = createDataBuffer(bufferPoolSize, bufferSize, 1);
        appendAndCheckResult(dataBuffer, bufferPoolSize * bufferSize + 1, true, 0, 0, false);
    }

    private void appendAndCheckResult(
            DataBuffer dataBuffer,
            int recordSize,
            boolean isFull,
            long numBytes,
            long numRecords,
            boolean hasRemaining)
            throws IOException {
        ByteBuffer largeRecord = ByteBuffer.allocate(recordSize);

        assertThat(dataBuffer.append(largeRecord, 0, Buffer.DataType.DATA_BUFFER))
                .isEqualTo(isFull);
        assertThat(dataBuffer.numTotalBytes()).isEqualTo(numBytes);
        assertThat(dataBuffer.numTotalRecords()).isEqualTo(numRecords);
        assertThat(dataBuffer.hasRemaining()).isEqualTo(hasRemaining);
    }

    @TestTemplate
    void testReadUnfinishedDataBuffer() throws Exception {
        int bufferSize = 1024;

        DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
        dataBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);

        assertThat(dataBuffer.hasRemaining()).isTrue();
        assertThatThrownBy(
                        () ->
                                dataBuffer.getNextBuffer(
                                        MemorySegmentFactory.allocateUnpooledSegment(bufferSize)))
                .isInstanceOf(IllegalStateException.class);
    }

    @TestTemplate
    void testReadReleasedDataBuffer() throws Exception {
        int bufferSize = 1024;

        DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
        dataBuffer.append(ByteBuffer.allocate(1), 0, Buffer.DataType.DATA_BUFFER);
        dataBuffer.finish();
        assertThat(dataBuffer.hasRemaining()).isTrue();

        dataBuffer.release();
        assertThat(dataBuffer.hasRemaining()).isTrue();

        assertThatThrownBy(
                        () ->
                                dataBuffer.getNextBuffer(
                                        MemorySegmentFactory.allocateUnpooledSegment(bufferSize)))
                .isInstanceOf(IllegalStateException.class);
    }

    @TestTemplate
    void testReadEmptyDataBuffer() throws Exception {
        int bufferSize = 1024;

        DataBuffer dataBuffer = createDataBuffer(1, bufferSize, 1);
        dataBuffer.finish();

        assertThat(dataBuffer.hasRemaining()).isFalse();
        assertThat(
                        dataBuffer.getNextBuffer(
                                MemorySegmentFactory.allocateUnpooledSegment(bufferSize)))
                .isNull();
    }

    @TestTemplate
    void testReleaseDataBuffer() throws Exception {
        int bufferPoolSize = 10;
        int bufferSize = 1024;
        int recordSize = (bufferPoolSize - 1) * bufferSize;

        NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, bufferSize);
        BufferPool bufferPool = globalPool.createBufferPool(bufferPoolSize, bufferPoolSize);

        LinkedList<MemorySegment> segments = new LinkedList<>();
        for (int i = 0; i < bufferPoolSize; ++i) {
            segments.add(bufferPool.requestMemorySegmentBlocking());
        }
        DataBuffer dataBuffer =
                new SortBasedDataBuffer(segments, bufferPool, 1, bufferSize, bufferPoolSize, null);
        dataBuffer.append(ByteBuffer.allocate(recordSize), 0, Buffer.DataType.DATA_BUFFER);

        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(bufferPoolSize);
        assertThat(dataBuffer.hasRemaining()).isTrue();
        assertThat(dataBuffer.numTotalRecords()).isOne();
        assertThat(dataBuffer.numTotalBytes()).isEqualTo(recordSize);

        // should release all data and resources
        dataBuffer.release();
        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isZero();
        assertThat(dataBuffer.hasRemaining()).isTrue();
        assertThat(dataBuffer.numTotalRecords()).isOne();
        assertThat(dataBuffer.numTotalBytes()).isEqualTo(recordSize);
    }

    private DataBuffer createDataBuffer(int bufferPoolSize, int bufferSize, int numSubpartitions)
            throws Exception {
        return createDataBuffer(bufferPoolSize, bufferSize, numSubpartitions, null);
    }

    private DataBuffer createDataBuffer(
            int bufferPoolSize, int bufferSize, int numSubpartitions, int[] customReadOrder)
            throws Exception {
        NetworkBufferPool globalPool = new NetworkBufferPool(bufferPoolSize, bufferSize);
        BufferPool bufferPool = globalPool.createBufferPool(bufferPoolSize, bufferPoolSize);

        LinkedList<MemorySegment> segments = new LinkedList<>();
        for (int i = 0; i < bufferPoolSize; ++i) {
            segments.add(bufferPool.requestMemorySegmentBlocking());
        }
        if (useHashBuffer) {
            return new HashBasedDataBuffer(
                    segments,
                    bufferPool,
                    numSubpartitions,
                    bufferSize,
                    bufferPoolSize,
                    customReadOrder);
        } else {
            return new SortBasedDataBuffer(
                    segments,
                    bufferPool,
                    numSubpartitions,
                    bufferSize,
                    bufferPoolSize,
                    customReadOrder);
        }
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
