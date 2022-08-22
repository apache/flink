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
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndex.SpilledBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSubpartitionFileReaderImpl.BufferIndexOrError;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HsSubpartitionFileReaderImpl}. */
@ExtendWith(TestLoggerExtension.class)
class HsSubpartitionFileReaderImplTest {
    private static final int bufferSize = Integer.BYTES;

    private static final int targetChannel = 0;

    private static final int MAX_BUFFERS_READ_AHEAD = 5;

    private Random random;

    private HsFileDataIndex diskIndex;

    private TestingSubpartitionViewInternalOperation subpartitionOperation;

    private FileChannel dataFileChannel;

    private long currentFileOffset;

    @BeforeEach
    void before(@TempDir Path tempPath) throws Exception {
        random = new Random();
        Path dataFilePath = Files.createFile(tempPath.resolve(UUID.randomUUID().toString()));
        dataFileChannel = openFileChannel(dataFilePath);
        diskIndex = new HsFileDataIndexImpl(1);
        subpartitionOperation = new TestingSubpartitionViewInternalOperation();
        currentFileOffset = 0L;
    }

    @AfterEach
    void after() {
        IOUtils.closeQuietly(dataFileChannel);
    }

    @Test
    void testReadBuffer() throws Exception {
        diskIndex = new HsFileDataIndexImpl(2);
        TestingSubpartitionViewInternalOperation viewNotifier1 =
                new TestingSubpartitionViewInternalOperation();
        TestingSubpartitionViewInternalOperation viewNotifier2 =
                new TestingSubpartitionViewInternalOperation();
        HsSubpartitionFileReaderImpl fileReader1 = createSubpartitionFileReader(0, viewNotifier1);
        HsSubpartitionFileReaderImpl fileReader2 = createSubpartitionFileReader(1, viewNotifier2);

        writeDataToFile(0, 0, 10, 2);
        writeDataToFile(1, 0, 20, 2);

        writeDataToFile(0, 2, 15, 1);
        writeDataToFile(1, 2, 25, 1);

        Queue<MemorySegment> memorySegments = createsMemorySegments(6);

        fileReader1.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);
        assertThat(memorySegments).hasSize(4);
        checkData(fileReader1, 10, 11);

        fileReader2.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);
        assertThat(memorySegments).hasSize(2);
        checkData(fileReader2, 20, 21);

        fileReader1.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);
        assertThat(memorySegments).hasSize(1);
        checkData(fileReader1, 15);

        fileReader2.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);
        assertThat(memorySegments).isEmpty();
        checkData(fileReader2, 25);
    }

    @ParameterizedTest
    @ValueSource(strings = {"LZ4", "LZO", "ZSTD"})
    void testReadBufferCompressed(String compressionFactoryName) throws Exception {
        BufferCompressor bufferCompressor =
                new BufferCompressor(bufferSize, compressionFactoryName);
        BufferDecompressor bufferDecompressor =
                new BufferDecompressor(bufferSize, compressionFactoryName);

        diskIndex = new HsFileDataIndexImpl(1);
        TestingSubpartitionViewInternalOperation viewNotifier =
                new TestingSubpartitionViewInternalOperation();
        HsSubpartitionFileReaderImpl fileReader1 = createSubpartitionFileReader(0, viewNotifier);

        writeDataToFile(0, 0, 1, 3, bufferCompressor);

        Queue<MemorySegment> memorySegments = createsMemorySegments(3);

        fileReader1.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);
        checkData(fileReader1, bufferDecompressor, 1, 2, 3);
    }

    @Test
    void testReadEmptyRegion() throws Exception {
        HsSubpartitionFileReaderImpl subpartitionFileReader = createSubpartitionFileReader();
        Deque<BufferIndexOrError> loadedBuffers = subpartitionFileReader.getLoadedBuffers();
        Queue<MemorySegment> memorySegments = createsMemorySegments(2);
        subpartitionFileReader.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);

        assertThat(memorySegments).hasSize(2);
        assertThat(loadedBuffers).isEmpty();
    }

    /**
     * If target buffer is not the first buffer in the region, file reader will skip the buffers not
     * needed.
     */
    @Test
    void testReadBufferSkip() throws Exception {
        HsSubpartitionFileReaderImpl subpartitionFileReader = createSubpartitionFileReader();
        Deque<BufferIndexOrError> loadedBuffers = subpartitionFileReader.getLoadedBuffers();
        // write buffer with index: 0, 1, 2, 3, 4, 5
        writeDataToFile(targetChannel, 0, 6);

        subpartitionOperation.advanceConsumptionProgress();
        subpartitionOperation.advanceConsumptionProgress();
        assertThat(subpartitionOperation.getConsumingOffset(true)).isEqualTo(1);
        // update consumptionProgress
        subpartitionFileReader.prepareForScheduling();
        // read buffer, expected buffer with index: 2
        Queue<MemorySegment> segments = createsMemorySegments(1);
        subpartitionFileReader.readBuffers(segments, FreeingBufferRecycler.INSTANCE);

        assertThat(segments).isEmpty();
        assertThat(loadedBuffers).hasSize(1);

        BufferIndexOrError bufferIndexOrError = loadedBuffers.poll();
        assertThat(bufferIndexOrError).isNotNull();
        assertThat(bufferIndexOrError.getBuffer()).isPresent();
        assertThat(bufferIndexOrError.getThrowable()).isNotPresent();
        assertThat(bufferIndexOrError.getIndex()).isEqualTo(2);

        subpartitionOperation.advanceConsumptionProgress();
        subpartitionOperation.advanceConsumptionProgress();
        subpartitionFileReader.prepareForScheduling();
        segments = createsMemorySegments(1);
        // trigger next round read, cached region will not update, but numSkip, numReadable and
        // currentBufferIndex should be updated.
        subpartitionFileReader.readBuffers(segments, FreeingBufferRecycler.INSTANCE);
        assertThat(segments).isEmpty();
        assertThat(loadedBuffers).hasSize(1);

        bufferIndexOrError = loadedBuffers.poll();
        assertThat(bufferIndexOrError).isNotNull();
        assertThat(bufferIndexOrError.getBuffer()).isPresent();
        assertThat(bufferIndexOrError.getThrowable()).isNotPresent();
        assertThat(bufferIndexOrError.getIndex()).isEqualTo(4);
    }

    @Test
    void testReadBufferNotBeyondRegionBoundary() throws Exception {
        HsSubpartitionFileReaderImpl subpartitionFileReader = createSubpartitionFileReader();
        Deque<BufferIndexOrError> loadedBuffers = subpartitionFileReader.getLoadedBuffers();

        // create two region: (0-0, 0-1) (0-2, 0-3)
        writeDataToFile(targetChannel, 0, 0, 2);
        writeDataToFile(targetChannel, 2, 2, 2);

        subpartitionFileReader.prepareForScheduling();

        // create enough buffers for read all two regions.
        Queue<MemorySegment> memorySegments = createsMemorySegments(4);
        // trigger reading
        subpartitionFileReader.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);

        assertThat(loadedBuffers).hasSize(2);
        checkData(subpartitionFileReader, 0, 1);
        assertThat(memorySegments).hasSize(2);
    }

    @Test
    void testReadBufferNotExceedThreshold() throws Exception {
        HsSubpartitionFileReaderImpl subpartitionFileReader = createSubpartitionFileReader();
        Deque<BufferIndexOrError> loadedBuffers = subpartitionFileReader.getLoadedBuffers();

        writeDataToFile(targetChannel, 0, MAX_BUFFERS_READ_AHEAD + 1);

        subpartitionFileReader.prepareForScheduling();
        // allocate maxBuffersReadAhead + 1 read buffers for reading
        Queue<MemorySegment> memorySegments = createsMemorySegments(MAX_BUFFERS_READ_AHEAD + 1);
        // trigger reading
        subpartitionFileReader.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);

        // preload by fileReader will not exceed threshold
        assertThat(loadedBuffers).hasSize(MAX_BUFFERS_READ_AHEAD);
        assertThat(memorySegments).hasSize(1);
    }

    @Test
    void testReadBufferNotifyDataAvailable() throws Exception {
        OneShotLatch notifyLatch = new OneShotLatch();
        subpartitionOperation.setNotifyDataAvailableRunnable(notifyLatch::trigger);

        HsSubpartitionFileReaderImpl subpartitionFileReader = createSubpartitionFileReader();
        BlockingDeque<BufferIndexOrError> loadedBuffers =
                (BlockingDeque<BufferIndexOrError>) subpartitionFileReader.getLoadedBuffers();

        // trigger next round reading.
        final int numBuffers = MAX_BUFFERS_READ_AHEAD;
        Queue<MemorySegment> memorySegments = createsMemorySegments(numBuffers);
        writeDataToFile(targetChannel, 0, numBuffers);
        subpartitionFileReader.prepareForScheduling();
        CheckedThread checkedThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        int numConsumedBuffer = 0;
                        while (numConsumedBuffer < numBuffers) {
                            BufferIndexOrError bufferIndexOrError = loadedBuffers.poll();
                            if (bufferIndexOrError != null) {
                                assertThat(bufferIndexOrError.getBuffer()).isPresent();
                                numConsumedBuffer++;
                            } else {
                                notifyLatch.await();
                                notifyLatch.reset();
                            }
                        }
                    }
                };
        checkedThread.start();

        // read data from disk then add it to buffer queue.
        subpartitionFileReader.readBuffers(memorySegments, FreeingBufferRecycler.INSTANCE);

        checkedThread.sync();

        assertThat(loadedBuffers).isEmpty();
    }

    @Test
    void testReadWillReturnBufferAfterError() throws Exception {
        HsSubpartitionFileReaderImpl subpartitionFileReader = createSubpartitionFileReader();
        writeDataToFile(targetChannel, 0, 2);

        subpartitionFileReader.prepareForScheduling();

        Queue<MemorySegment> memorySegments = createsMemorySegments(2);
        // close data channel to trigger a error during read buffer.
        dataFileChannel.close();

        assertThatThrownBy(
                        () ->
                                subpartitionFileReader.readBuffers(
                                        memorySegments, FreeingBufferRecycler.INSTANCE))
                .isInstanceOf(IOException.class);

        assertThat(memorySegments).hasSize(2);
    }

    @Test
    void testReadBufferAfterFail() {
        HsSubpartitionFileReaderImpl subpartitionFileReader = createSubpartitionFileReader();
        subpartitionFileReader.fail(new RuntimeException("expected exception."));
        assertThatThrownBy(
                        () ->
                                subpartitionFileReader.readBuffers(
                                        createsMemorySegments(2), FreeingBufferRecycler.INSTANCE))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("subpartition reader has already failed.");
    }

    @Test
    void testFail() throws Exception {
        AtomicInteger numOfNotify = new AtomicInteger(0);
        subpartitionOperation.setNotifyDataAvailableRunnable(numOfNotify::incrementAndGet);
        HsSubpartitionFileReaderImpl subpartitionFileReader = createSubpartitionFileReader();
        Deque<BufferIndexOrError> loadedBuffers = subpartitionFileReader.getLoadedBuffers();
        writeDataToFile(targetChannel, 0, 2);

        subpartitionFileReader.prepareForScheduling();
        Queue<MemorySegment> memorySegments = createsMemorySegments(2);
        // trigger reading, add buffer to queue.
        AtomicInteger numReleased = new AtomicInteger(0);
        subpartitionFileReader.readBuffers(
                memorySegments, (buffer) -> numReleased.incrementAndGet());

        assertThat(memorySegments).isEmpty();
        assertThat(loadedBuffers).hasSize(2);
        assertThat(numOfNotify).hasValue(1);

        subpartitionFileReader.fail(new RuntimeException("expected exception."));
        // all buffers in file reader queue should recycle during fail.
        assertThat(numReleased).hasValue(2);
        BufferIndexOrError error = loadedBuffers.poll();
        assertThat(loadedBuffers).isEmpty();
        assertThat(error).isNotNull();
        assertThat(error.getThrowable())
                .hasValueSatisfying(
                        throwable ->
                                assertThat(throwable)
                                        .isInstanceOf(RuntimeException.class)
                                        .hasMessage("expected exception."));

        // subpartitionReader fail should notify downstream.
        assertThat(numOfNotify).hasValue(2);
    }

    @Test
    void testCompareTo() throws Exception {
        diskIndex = new HsFileDataIndexImpl(2);
        TestingSubpartitionViewInternalOperation viewNotifier1 =
                new TestingSubpartitionViewInternalOperation();
        TestingSubpartitionViewInternalOperation viewNotifier2 =
                new TestingSubpartitionViewInternalOperation();
        HsSubpartitionFileReaderImpl fileReader1 = createSubpartitionFileReader(0, viewNotifier1);
        HsSubpartitionFileReaderImpl fileReader2 = createSubpartitionFileReader(1, viewNotifier2);
        assertThat(fileReader1).isEqualByComparingTo(fileReader2);

        // buffers in file: (0-0, 0-1, 1-0, 0-2)
        writeDataToFile(0, 0, 2);
        writeDataToFile(1, 0, 1);
        writeDataToFile(0, 2, 1);
        // fileReader1 -> (0-0) fileReader2 -> (1-0)
        assertThat(fileReader1).isLessThan(fileReader2);

        viewNotifier1.advanceConsumptionProgress();
        fileReader1.prepareForScheduling();
        // fileReader1 -> (0-1) fileReader2 -> (1-0)
        assertThat(fileReader1).isLessThan(fileReader2);

        viewNotifier1.advanceConsumptionProgress();
        fileReader1.prepareForScheduling();
        // fileReader1 -> (0-2) fileReader2 -> (1-0)
        assertThat(fileReader1).isGreaterThan(fileReader2);
    }

    @Test
    void testConsumeBuffer() throws Throwable {
        TestingSubpartitionViewInternalOperation viewNotifier =
                new TestingSubpartitionViewInternalOperation();
        HsSubpartitionFileReaderImpl subpartitionFileReader =
                createSubpartitionFileReader(0, viewNotifier);

        // if no preload data in file reader, return Optional.empty.
        assertThat(subpartitionFileReader.consumeBuffer(0)).isNotPresent();

        // buffers in file: (0-0, 0-1)
        writeDataToFile(0, 0, 0, 2);

        Queue<MemorySegment> memorySegments = createsMemorySegments(2);
        // trigger reading, add buffer to queue.
        subpartitionFileReader.readBuffers(memorySegments, (ignore) -> {});

        // if nextBufferToConsume is not equal to peek elements index, return Optional.empty.
        assertThat(subpartitionFileReader.consumeBuffer(10)).isNotPresent();

        assertThat(subpartitionFileReader.consumeBuffer(0))
                .hasValueSatisfying(
                        (bufferAndBacklog -> {
                            assertThat(bufferAndBacklog.getNextDataType())
                                    .isEqualTo(DataType.EVENT_BUFFER);
                            assertThat(bufferAndBacklog.getSequenceNumber()).isEqualTo(0);
                            // first buffer's data is 0.
                            assertThat(
                                            bufferAndBacklog
                                                    .buffer()
                                                    .getNioBufferReadable()
                                                    .order(ByteOrder.nativeOrder())
                                                    .getInt())
                                    .isEqualTo(0);
                        }));
    }

    @Test
    void testPeekNextToConsumeDataTypeOrConsumeBufferThrowException() {
        TestingSubpartitionViewInternalOperation viewNotifier =
                new TestingSubpartitionViewInternalOperation();
        HsSubpartitionFileReaderImpl subpartitionFileReader =
                createSubpartitionFileReader(0, viewNotifier);

        subpartitionFileReader.fail(new RuntimeException("expected exception."));

        assertThatThrownBy(() -> subpartitionFileReader.peekNextToConsumeDataType(0))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("expected exception.");

        assertThatThrownBy(() -> subpartitionFileReader.consumeBuffer(0))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("expected exception.");
    }

    @Test
    void testPeekNextToConsumeDataType() throws Throwable {
        TestingSubpartitionViewInternalOperation viewNotifier =
                new TestingSubpartitionViewInternalOperation();
        HsSubpartitionFileReaderImpl subpartitionFileReader =
                createSubpartitionFileReader(0, viewNotifier);

        // if no preload data in file reader, return DataType.NONE.
        assertThat(subpartitionFileReader.peekNextToConsumeDataType(0)).isEqualTo(DataType.NONE);

        // buffers in file: (0-0, 0-1)
        writeDataToFile(0, 0, 2);

        Queue<MemorySegment> memorySegments = createsMemorySegments(2);
        // trigger reading, add buffer to queue.
        subpartitionFileReader.readBuffers(memorySegments, (ignore) -> {});

        // if nextBufferToConsume is not equal to peek elements index, return DataType.NONE.
        assertThat(subpartitionFileReader.peekNextToConsumeDataType(10)).isEqualTo(DataType.NONE);

        // if nextBufferToConsume is equal to peek elements index, return the real DataType.
        assertThat(subpartitionFileReader.peekNextToConsumeDataType(0))
                .isEqualTo(DataType.DATA_BUFFER);
    }

    private static void checkData(
            HsSubpartitionFileReaderImpl fileReader,
            BufferDecompressor bufferDecompressor,
            int... expectedData) {
        assertThat(fileReader.getLoadedBuffers()).hasSameSizeAs(expectedData);
        for (int data : expectedData) {
            BufferIndexOrError bufferIndexOrError = fileReader.getLoadedBuffers().poll();
            assertThat(bufferIndexOrError).isNotNull();
            assertThat(bufferIndexOrError.getBuffer()).isPresent();
            Buffer buffer = bufferIndexOrError.getBuffer().get();
            buffer =
                    buffer.isCompressed() && bufferDecompressor != null
                            ? bufferDecompressor.decompressToIntermediateBuffer(buffer)
                            : buffer;
            assertThat(buffer.getNioBufferReadable().order(ByteOrder.nativeOrder()).getInt())
                    .isEqualTo(data);
        }
    }

    private static void checkData(HsSubpartitionFileReaderImpl fileReader, int... expectedData) {
        checkData(fileReader, null, expectedData);
    }

    private HsSubpartitionFileReaderImpl createSubpartitionFileReader() {
        return createSubpartitionFileReader(targetChannel, subpartitionOperation);
    }

    private HsSubpartitionFileReaderImpl createSubpartitionFileReader(
            int targetChannel, HsSubpartitionViewInternalOperations operations) {
        return new HsSubpartitionFileReaderImpl(
                targetChannel,
                dataFileChannel,
                operations,
                diskIndex,
                MAX_BUFFERS_READ_AHEAD,
                (ignore) -> {},
                BufferReaderWriterUtil.allocatedHeaderBuffer());
    }

    private static FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private static Queue<MemorySegment> createsMemorySegments(int numSegments) {
        Queue<MemorySegment> segments = new ArrayDeque<>();
        for (int i = 0; i < numSegments; ++i) {
            segments.add(MemorySegmentFactory.allocateUnpooledSegment(bufferSize));
        }
        return segments;
    }

    private void writeDataToFile(
            int subpartitionId,
            int firstBufferIndex,
            int firstBufferData,
            int numBuffers,
            BufferCompressor bufferCompressor)
            throws Exception {
        List<SpilledBuffer> spilledBuffers = new ArrayList<>(numBuffers);
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * numBuffers];
        int totalBytes = 0;

        for (int i = 0; i < numBuffers; i++) {
            Buffer.DataType dataType =
                    i == numBuffers - 1
                            ? Buffer.DataType.EVENT_BUFFER
                            : Buffer.DataType.DATA_BUFFER;

            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
            segment.putInt(0, firstBufferData + i);
            Buffer buffer =
                    new NetworkBuffer(
                            segment, FreeingBufferRecycler.INSTANCE, dataType, bufferSize);
            if (bufferCompressor != null && buffer.isBuffer()) {
                buffer = bufferCompressor.compressToOriginalBuffer(buffer);
            }
            setBufferWithHeader(buffer, bufferWithHeaders, 2 * i);
            spilledBuffers.add(
                    new SpilledBuffer(
                            subpartitionId, firstBufferIndex + i, currentFileOffset + totalBytes));
            totalBytes += buffer.getSize() + BufferReaderWriterUtil.HEADER_LENGTH;
        }

        BufferReaderWriterUtil.writeBuffers(dataFileChannel, totalBytes, bufferWithHeaders);
        currentFileOffset += totalBytes;

        diskIndex.addBuffers(spilledBuffers);
        // mark all buffers status to release.
        spilledBuffers.forEach(
                spilledBuffer ->
                        diskIndex.markBufferReadable(subpartitionId, spilledBuffer.bufferIndex));
    }

    private void writeDataToFile(
            int subpartitionId, int firstBufferIndex, int firstBufferData, int numBuffers)
            throws Exception {
        writeDataToFile(subpartitionId, firstBufferIndex, firstBufferData, numBuffers, null);
    }

    private void writeDataToFile(int subpartitionId, int firstBufferIndex, int numBuffers)
            throws Exception {
        writeDataToFile(subpartitionId, firstBufferIndex, random.nextInt(), numBuffers);
    }

    private static void setBufferWithHeader(
            Buffer buffer, ByteBuffer[] bufferWithHeaders, int index) {
        ByteBuffer header = BufferReaderWriterUtil.allocatedHeaderBuffer();
        BufferReaderWriterUtil.setByteChannelBufferHeader(buffer, header);

        bufferWithHeaders[index] = header;
        bufferWithHeaders[index + 1] = buffer.getNioBufferReadable();
    }
}
