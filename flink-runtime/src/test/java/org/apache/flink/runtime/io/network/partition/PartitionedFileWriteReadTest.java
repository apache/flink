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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.IOUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for writing and reading {@link PartitionedFile} with {@link PartitionedFileWriter} and
 * {@link PartitionedFileReader}.
 */
class PartitionedFileWriteReadTest {
    private @TempDir Path tempPath;
    // We need a reference to the PartitionedFile to call deleteQuietly() after the test
    private PartitionedFile partitionedFile;

    @AfterEach
    void tearDown() {
        if (partitionedFile != null) {
            partitionedFile.deleteQuietly();
        }
    }

    @Test
    void testWriteAndReadPartitionedFile() throws Exception {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        int numBuffers = 1000;
        int numRegions = 10;
        Random random = new Random(1111);

        List<Buffer>[] buffersWritten = new List[numSubpartitions];
        List<Buffer>[] buffersRead = new List[numSubpartitions];
        List<Tuple2<Long, Long>>[] regionStat = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            buffersWritten[subpartition] = new ArrayList<>();
            buffersRead[subpartition] = new ArrayList<>();
            regionStat[subpartition] = new ArrayList<>();
        }

        int[] writeOrder = DataBufferTest.getRandomSubpartitionOrder(numSubpartitions);
        createPartitionedFile(
                numSubpartitions,
                bufferSize,
                numBuffers,
                numRegions,
                buffersWritten,
                regionStat,
                createPartitionedFileWriter(numSubpartitions, writeOrder),
                subpartitionIndex -> subpartitionIndex,
                random.nextBoolean(),
                writeOrder);

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            PartitionedFileReader fileReader =
                    new PartitionedFileReader(
                            partitionedFile,
                            new ResultSubpartitionIndexSet(subpartition),
                            dataFileChannel,
                            indexFileChannel,
                            BufferReaderWriterUtil.allocatedHeaderBuffer(),
                            createAndConfigIndexEntryBuffer(),
                            writeOrder[0]);
            while (fileReader.hasRemaining()) {
                final int subIndex = subpartition;
                fileReader.readCurrentRegion(
                        allocateBuffers(bufferSize),
                        FreeingBufferRecycler.INSTANCE,
                        buffer -> addReadBuffer(buffer, buffersRead[subIndex]));
            }
        }
        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);

        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            assertThat(buffersWritten[subpartition]).hasSameSizeAs(buffersRead[subpartition]);
            for (int i = 0; i < buffersWritten[subpartition].size(); ++i) {
                assertBufferEquals(
                        buffersWritten[subpartition].get(i), buffersRead[subpartition].get(i));
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    void testComputeReadablePosition(boolean randomSubpartitionOrder, boolean broadcastRegion)
            throws IOException {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        int numBuffers = 1000;
        int numRegions = 1;

        List<Buffer>[] buffersWritten = new List[numSubpartitions];
        List<Buffer>[] buffersRead = new List[numSubpartitions];
        List<Tuple2<Long, Long>>[] regionStat = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            buffersWritten[subpartition] = new ArrayList<>();
            buffersRead[subpartition] = new ArrayList<>();
            regionStat[subpartition] = new ArrayList<>();
        }

        int[] writeOrder =
                randomSubpartitionOrder
                        ? DataBufferTest.getRandomSubpartitionOrder(numSubpartitions)
                        : new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        createPartitionedFile(
                numSubpartitions,
                bufferSize,
                numBuffers,
                numRegions,
                buffersWritten,
                regionStat,
                createPartitionedFileWriter(numSubpartitions, writeOrder),
                subpartitionIndex -> subpartitionIndex,
                broadcastRegion,
                writeOrder);

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());

        verifyReadablePosition(
                0,
                numSubpartitions - 1,
                writeOrder[0],
                dataFileChannel,
                indexFileChannel,
                partitionedFile,
                regionStat,
                broadcastRegion);

        verifyReadablePosition(
                0,
                writeOrder[0],
                writeOrder[0],
                dataFileChannel,
                indexFileChannel,
                partitionedFile,
                regionStat,
                broadcastRegion);

        verifyReadablePosition(
                writeOrder[0],
                numSubpartitions - 1,
                writeOrder[0],
                dataFileChannel,
                indexFileChannel,
                partitionedFile,
                regionStat,
                broadcastRegion);
    }

    private void verifyReadablePosition(
            int start,
            int end,
            int subpartitionOrderRotationIndex,
            FileChannel dataFileChannel,
            FileChannel indexFileChannel,
            PartitionedFile partitionedFile,
            List<Tuple2<Long, Long>>[] regionStat,
            boolean isBroadcastRegion)
            throws IOException {
        PartitionedFileReader fileReader =
                new PartitionedFileReader(
                        partitionedFile,
                        new ResultSubpartitionIndexSet(start, end),
                        dataFileChannel,
                        indexFileChannel,
                        BufferReaderWriterUtil.allocatedHeaderBuffer(),
                        createAndConfigIndexEntryBuffer(),
                        subpartitionOrderRotationIndex);

        Queue<PartitionedFileReader.BufferPositionDescriptor> offsetAndSizesToRead =
                new ArrayDeque<>();
        fileReader.updateReadableOffsetAndSize(
                createAndConfigIndexEntryBuffer(), offsetAndSizesToRead);

        if (isBroadcastRegion) {
            assertThat(
                            offsetAndSizesToRead.stream()
                                    .map(
                                            PartitionedFileReader.BufferPositionDescriptor
                                                    ::getRepeatCount)
                                    .reduce(Integer::sum)
                                    .get())
                    .isEqualTo(end - start + 1);
            for (PartitionedFileReader.BufferPositionDescriptor descriptor : offsetAndSizesToRead) {
                assertThat(descriptor.getOffset()).isEqualTo(regionStat[start].get(0).f0);
                assertThat(descriptor.getSize()).isEqualTo(regionStat[start].get(0).f1);
            }
            return;
        }

        if (start >= subpartitionOrderRotationIndex || end <= subpartitionOrderRotationIndex - 1) {
            assertThat(offsetAndSizesToRead).hasSize(1);

            PartitionedFileReader.BufferPositionDescriptor descriptor = offsetAndSizesToRead.poll();
            assertThat(descriptor.getOffset()).isEqualTo(regionStat[start].get(0).f0);

            long expectedSize = 0L;
            for (int i = start; i <= end; i++) {
                expectedSize += regionStat[i].get(0).f1;
            }
            assertThat(descriptor.getSize()).isEqualTo(expectedSize);
        } else {
            assertThat(offsetAndSizesToRead).hasSize(2);

            PartitionedFileReader.BufferPositionDescriptor descriptor1 =
                    offsetAndSizesToRead.poll();
            PartitionedFileReader.BufferPositionDescriptor descriptor2 =
                    offsetAndSizesToRead.poll();
            assertThat(descriptor1.getOffset())
                    .isEqualTo(regionStat[subpartitionOrderRotationIndex].get(0).f0);
            assertThat(descriptor2.getOffset()).isEqualTo(regionStat[start].get(0).f0);

            long expectedSize = 0L;
            for (int i = subpartitionOrderRotationIndex; i <= end; i++) {
                expectedSize += regionStat[i].get(0).f1;
            }
            assertThat(descriptor1.getSize()).isEqualTo(expectedSize);

            expectedSize = 0L;
            for (int i = start; i < subpartitionOrderRotationIndex; i++) {
                expectedSize += regionStat[i].get(0).f1;
            }
            assertThat(descriptor2.getSize()).isEqualTo(expectedSize);
        }
    }

    @Test
    void testWriteAndReadPartitionedFileForSubpartitionRange() throws Exception {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        int numBuffers = 1000;
        int numRegions = 10;

        List<Buffer>[] buffersWritten = new List[numSubpartitions];
        List<Buffer>[] buffersRead = new List[numSubpartitions / 2];
        List<Tuple2<Long, Long>>[] regionStat = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            if (subpartition % 2 == 0) {
                buffersWritten[subpartition / 2] = new ArrayList<>();
                buffersRead[subpartition / 2] = new ArrayList<>();
            }
            regionStat[subpartition] = new ArrayList<>();
        }

        int[] writeOrder = DataBufferTest.getRandomSubpartitionOrder(numSubpartitions);
        createPartitionedFile(
                numSubpartitions,
                bufferSize,
                numBuffers,
                numRegions,
                buffersWritten,
                regionStat,
                createPartitionedFileWriter(numSubpartitions, writeOrder),
                subpartitionIndex -> subpartitionIndex / 2,
                false,
                writeOrder);

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());

        for (int subpartition = 0; subpartition < numSubpartitions; subpartition += 2) {
            PartitionedFileReader fileReader =
                    new PartitionedFileReader(
                            partitionedFile,
                            new ResultSubpartitionIndexSet(subpartition, subpartition + 1),
                            dataFileChannel,
                            indexFileChannel,
                            BufferReaderWriterUtil.allocatedHeaderBuffer(),
                            createAndConfigIndexEntryBuffer(),
                            writeOrder[0]);
            while (fileReader.hasRemaining()) {
                final int subIndex = subpartition;
                fileReader.readCurrentRegion(
                        allocateBuffers(bufferSize),
                        FreeingBufferRecycler.INSTANCE,
                        buffer -> addReadBuffer(buffer, buffersRead[subIndex / 2]));
            }
        }
        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);

        for (int subpartition = 0; subpartition < numSubpartitions; subpartition += 2) {
            assertThat(buffersWritten[subpartition / 2])
                    .hasSameSizeAs(buffersRead[subpartition / 2]);

            for (int i = 0; i < buffersRead[subpartition / 2].size(); ++i) {
                assertBufferEquals(
                        buffersWritten[subpartition / 2].get(i),
                        buffersRead[subpartition / 2].get(i));
            }
        }
    }

    private void createPartitionedFile(
            int numSubpartitions,
            int bufferSize,
            int numBuffers,
            int numRegions,
            List<Buffer>[] buffersWritten,
            List<Tuple2<Long, Long>>[] regionStat,
            PartitionedFileWriter fileWriter,
            Function<Integer, Integer> writtenIndexRetriever,
            boolean isBroadcastRegion,
            int[] writeOrder)
            throws IOException {
        Random random = new Random(1111);
        long currentOffset = 0L;
        for (int region = 0; region < numRegions; ++region) {
            fileWriter.startNewRegion(isBroadcastRegion);
            List<BufferWithSubpartition>[] bufferWithSubpartitions = new List[numSubpartitions];
            for (int i = 0; i < numSubpartitions; i++) {
                bufferWithSubpartitions[i] = new ArrayList<>();
            }

            for (int i = 0; i < numBuffers; ++i) {
                Buffer buffer = createBuffer(random, bufferSize);
                if (isBroadcastRegion) {
                    for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                        bufferWithSubpartitions[subpartition].add(
                                new BufferWithSubpartition(buffer, subpartition));
                    }
                } else {
                    int subpartition = random.nextInt(numSubpartitions);
                    bufferWithSubpartitions[subpartition].add(
                            new BufferWithSubpartition(buffer, subpartition));
                }
            }

            for (int index = 0; index < numSubpartitions; ++index) {
                int subpartition = writeOrder[index];
                fileWriter.writeBuffers(bufferWithSubpartitions[subpartition]);

                List<Buffer> writtenBuffer =
                        bufferWithSubpartitions[subpartition].stream()
                                .map(BufferWithSubpartition::getBuffer)
                                .collect(Collectors.toList());
                int writtenIndex = writtenIndexRetriever.apply(subpartition);
                buffersWritten[writtenIndex].addAll(writtenBuffer);
                long totalBytes = getTotalBytes(bufferWithSubpartitions[subpartition]);
                if (isBroadcastRegion) {
                    for (int j = 0; j < numSubpartitions; j++) {
                        regionStat[j].add(Tuple2.of(currentOffset, totalBytes));

                        if (j != writtenIndex) {
                            buffersWritten[j].addAll(writtenBuffer);
                        }
                    }
                    currentOffset += totalBytes;
                    break;
                } else {
                    regionStat[subpartition].add(Tuple2.of(currentOffset, totalBytes));
                    currentOffset += totalBytes;
                }
            }
        }
        partitionedFile = fileWriter.finish();
    }

    private static long getTotalBytes(List<BufferWithSubpartition> bufferWithSubpartitions) {
        long totalBytes = 0L;
        for (BufferWithSubpartition bufferWithSubpartition : bufferWithSubpartitions) {
            totalBytes +=
                    bufferWithSubpartition.getBuffer().readableBytes()
                            + BufferReaderWriterUtil.HEADER_LENGTH;
        }
        return totalBytes;
    }

    private void addReadBuffer(Buffer buffer, List<Buffer> buffersRead) {
        int numBytes = buffer.readableBytes();
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(numBytes);
        Buffer fullBuffer =
                ((CompositeBuffer) buffer)
                        .getFullBufferData(MemorySegmentFactory.allocateUnpooledSegment(numBytes));
        segment.put(0, fullBuffer.getNioBufferReadable(), fullBuffer.readableBytes());
        buffersRead.add(
                new NetworkBuffer(
                        segment,
                        ignore -> {},
                        fullBuffer.getDataType(),
                        fullBuffer.isCompressed(),
                        fullBuffer.readableBytes()));
        fullBuffer.recycleBuffer();
    }

    private static Queue<MemorySegment> allocateBuffers(int bufferSize) {
        return allocateBuffers(bufferSize, 2);
    }

    private static Queue<MemorySegment> allocateBuffers(int bufferSize, int numBuffers) {
        Queue<MemorySegment> readBuffers = new LinkedList<>();
        while (numBuffers-- > 0) {
            readBuffers.add(MemorySegmentFactory.allocateUnpooledSegment(bufferSize));
        }
        return readBuffers;
    }

    @Test
    void testWriteAndReadWithEmptySubpartition() throws Exception {
        int numRegions = 10;
        int numSubpartitions = 5;
        int bufferSize = 1024;
        Random random = new Random(1111);

        Queue<Buffer>[] subpartitionBuffers = new ArrayDeque[numSubpartitions];
        List<Buffer>[] buffersRead = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            subpartitionBuffers[subpartition] = new ArrayDeque<>();
            buffersRead[subpartition] = new ArrayList<>();
        }

        PartitionedFileWriter fileWriter =
                createPartitionedFileWriter(numSubpartitions, new int[] {0, 1, 2, 3, 4});
        for (int region = 0; region < numRegions; ++region) {
            fileWriter.startNewRegion(false);
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                if (random.nextBoolean()) {
                    Buffer buffer = createBuffer(random, bufferSize);
                    subpartitionBuffers[subpartition].add(buffer);
                    fileWriter.writeBuffers(getBufferWithSubpartitions(buffer, subpartition));
                }
            }
        }
        partitionedFile = fileWriter.finish();

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            PartitionedFileReader fileReader =
                    new PartitionedFileReader(
                            partitionedFile,
                            new ResultSubpartitionIndexSet(subpartition),
                            dataFileChannel,
                            indexFileChannel,
                            BufferReaderWriterUtil.allocatedHeaderBuffer(),
                            createAndConfigIndexEntryBuffer(),
                            0);
            int bufferIndex = 0;
            while (fileReader.hasRemaining()) {
                final int subIndex = subpartition;
                fileReader.readCurrentRegion(
                        allocateBuffers(bufferSize),
                        FreeingBufferRecycler.INSTANCE,
                        buffer -> addReadBuffer(buffer, buffersRead[subIndex]));
                Buffer buffer = buffersRead[subIndex].get(bufferIndex++);
                assertBufferEquals(checkNotNull(subpartitionBuffers[subpartition].poll()), buffer);
            }
            assertThat(subpartitionBuffers[subpartition]).isEmpty();
        }
        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);
    }

    @Test
    void testWriteAndReadWithEmptySubpartitionForMultipleSubpartitions() throws Exception {
        int numRegions = 10;
        int numSubpartitions = 5;
        int bufferSize = 1024;
        Random random = new Random();

        Queue<Buffer>[] subpartitionBuffers = new ArrayDeque[numRegions];
        List<Buffer>[] buffersRead = new List[numRegions];
        for (int region = 0; region < numRegions; region++) {
            subpartitionBuffers[region] = new ArrayDeque<>();
            buffersRead[region] = new ArrayList<>();
        }

        int[] writeOrder = new int[] {0, 1, 2, 3, 4};
        PartitionedFileWriter fileWriter =
                createPartitionedFileWriter(numSubpartitions, writeOrder);
        for (int region = 0; region < numRegions; ++region) {
            fileWriter.startNewRegion(false);
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                if (random.nextBoolean()) {
                    Buffer buffer = createBuffer(random, bufferSize);
                    subpartitionBuffers[region].add(buffer);
                    fileWriter.writeBuffers(getBufferWithSubpartitions(buffer, subpartition));
                }
            }
        }
        partitionedFile = fileWriter.finish();

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        PartitionedFileReader fileReader =
                new PartitionedFileReader(
                        partitionedFile,
                        new ResultSubpartitionIndexSet(0, numSubpartitions - 1),
                        dataFileChannel,
                        indexFileChannel,
                        BufferReaderWriterUtil.allocatedHeaderBuffer(),
                        createAndConfigIndexEntryBuffer(),
                        writeOrder[0]);
        int regionIndex = 0;
        while (fileReader.hasRemaining()) {
            if (subpartitionBuffers[regionIndex].isEmpty()) {
                regionIndex++;
            } else {
                int finalRegionIndex = regionIndex;
                fileReader.readCurrentRegion(
                        allocateBuffers(bufferSize, 10),
                        FreeingBufferRecycler.INSTANCE,
                        buffer -> addReadBuffer(buffer, buffersRead[finalRegionIndex]));
                for (Buffer buffer : buffersRead[finalRegionIndex]) {
                    assertBufferEquals(
                            checkNotNull(subpartitionBuffers[finalRegionIndex].poll()), buffer);
                }

                assertThat(subpartitionBuffers[finalRegionIndex]).isEmpty();
                regionIndex++;
            }
        }
        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);
    }

    private void assertBufferEquals(Buffer expected, Buffer actual) {
        assertThat(expected.getDataType()).isEqualTo(actual.getDataType());
        assertThat(expected.getNioBufferReadable()).isEqualTo(actual.getNioBufferReadable());
    }

    private Buffer createBuffer(Random random, int bufferSize) {
        boolean isBuffer = random.nextBoolean();
        Buffer.DataType dataType =
                isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;

        int dataSize = random.nextInt(bufferSize) + 1;
        byte[] data = new byte[dataSize];
        return new NetworkBuffer(MemorySegmentFactory.wrap(data), (buf) -> {}, dataType, dataSize);
    }

    @Test
    void testNotWriteDataOfTheSameSubpartitionTogether() throws Exception {
        PartitionedFileWriter partitionedFileWriter =
                createPartitionedFileWriter(2, new int[] {1, 0});
        try {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);

            assertThatThrownBy(
                            () -> {
                                NetworkBuffer buffer1 = new NetworkBuffer(segment, (buf) -> {});
                                partitionedFileWriter.writeBuffers(
                                        getBufferWithSubpartitions(buffer1, 1));

                                NetworkBuffer buffer2 = new NetworkBuffer(segment, (buf) -> {});
                                partitionedFileWriter.writeBuffers(
                                        getBufferWithSubpartitions(buffer2, 0));

                                NetworkBuffer buffer3 = new NetworkBuffer(segment, (buf) -> {});
                                partitionedFileWriter.writeBuffers(
                                        getBufferWithSubpartitions(buffer3, 1));
                            })
                    .isInstanceOf(IllegalStateException.class);

        } finally {
            partitionedFile = partitionedFileWriter.finish();
        }
    }

    @Test
    void testWriteFinishedPartitionedFile() throws Exception {
        PartitionedFileWriter partitionedFileWriter = createAndFinishPartitionedFileWriter();

        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
        NetworkBuffer buffer = new NetworkBuffer(segment, (buf) -> {});

        assertThatThrownBy(
                        () ->
                                partitionedFileWriter.writeBuffers(
                                        getBufferWithSubpartitions(buffer, 0)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testFinishPartitionedFileWriterTwice() throws Exception {
        PartitionedFileWriter partitionedFileWriter = createAndFinishPartitionedFileWriter();
        assertThatThrownBy(() -> partitionedFileWriter.finish())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testReadEmptyPartitionedFile() throws Exception {
        int bufferSize = 1024;
        int numSubpartitions = 2;
        int targetSubpartition = 1;
        createEmptyPartitionedFile();

        List<Buffer>[] buffersRead = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            buffersRead[subpartition] = new ArrayList<>();
        }

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        PartitionedFileReader partitionedFileReader =
                new PartitionedFileReader(
                        partitionedFile,
                        new ResultSubpartitionIndexSet(1),
                        dataFileChannel,
                        indexFileChannel,
                        BufferReaderWriterUtil.allocatedHeaderBuffer(),
                        createAndConfigIndexEntryBuffer(),
                        0);

        partitionedFileReader.readCurrentRegion(
                allocateBuffers(bufferSize),
                FreeingBufferRecycler.INSTANCE,
                buffer -> addReadBuffer(buffer, buffersRead[targetSubpartition]));
        assertThat(buffersRead[targetSubpartition]).isEmpty();
        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);
    }

    /**
     * For <a
     * href="https://issues.apache.org/jira/projects/FLINK/issues/FLINK-32027">FLINK-32027</a>.
     */
    @Test
    void testMultipleThreadGetIndexEntry() throws Exception {
        final int numSubpartitions = 5;
        final int bufferSize = 1024;
        final int numBuffers = 100;
        final int numRegions = 10;
        Random random = new Random(1111);

        List<Buffer>[] buffersWritten = new List[numSubpartitions];
        List<Buffer>[] buffersRead = new List[numSubpartitions];
        List<Tuple2<Long, Long>>[] regionStat = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            buffersWritten[subpartition] = new ArrayList<>();
            buffersRead[subpartition] = new ArrayList<>();
            regionStat[subpartition] = new ArrayList<>();
        }

        int[] writeOrder = DataBufferTest.getRandomSubpartitionOrder(numSubpartitions);
        createPartitionedFile(
                numSubpartitions,
                bufferSize,
                numBuffers,
                numRegions,
                buffersWritten,
                regionStat,
                createPartitionedFileWriter(
                        numSubpartitions,
                        PartitionedFile.INDEX_ENTRY_SIZE * numSubpartitions,
                        PartitionedFile.INDEX_ENTRY_SIZE * numSubpartitions,
                        writeOrder),
                subpartitionIndex -> subpartitionIndex,
                random.nextBoolean(),
                writeOrder);

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());

        CheckedThread[] readers = new CheckedThread[numSubpartitions];
        for (int i = 0; i < numSubpartitions; i++) {
            final int subpartition = i;
            readers[i] =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            ByteBuffer indexEntryBuffer = createAndConfigIndexEntryBuffer();
                            for (int region = 0; region < numRegions; region++) {
                                partitionedFile.getIndexEntry(
                                        indexFileChannel, indexEntryBuffer, region, subpartition);
                                long offset = indexEntryBuffer.getLong();
                                long regionBytes = indexEntryBuffer.getLong();
                                assertThat(offset)
                                        .isEqualTo(regionStat[subpartition].get(region).f0);
                                assertThat(regionBytes)
                                        .isEqualTo(regionStat[subpartition].get(region).f1);
                            }
                        }
                    };
        }

        for (CheckedThread reader : readers) {
            reader.start();
        }
        for (CheckedThread reader : readers) {
            reader.sync();
        }

        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);
    }

    private FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private List<BufferWithSubpartition> getBufferWithSubpartitions(
            Buffer buffer, int subpartitionIndex) {
        return Collections.singletonList(new BufferWithSubpartition(buffer, subpartitionIndex));
    }

    private void createEmptyPartitionedFile() throws IOException {
        PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(2, new int[0]);
        partitionedFile = partitionedFileWriter.finish();
    }

    private PartitionedFileWriter createPartitionedFileWriter(
            int numSubpartitions, int[] writeOrder) throws IOException {
        return createPartitionedFileWriter(numSubpartitions, 640, writeOrder);
    }

    private PartitionedFileWriter createPartitionedFileWriter(
            int numSubpartitions, int minIndexBufferSize, int maxIndexBufferSize, int[] writeOrder)
            throws IOException {
        return new PartitionedFileWriter(
                numSubpartitions,
                minIndexBufferSize,
                maxIndexBufferSize,
                tempPath.toString(),
                writeOrder);
    }

    private PartitionedFileWriter createPartitionedFileWriter(
            int numSubpartitions, int maxIndexBufferSize, int[] writeOrder) throws IOException {
        return new PartitionedFileWriter(
                numSubpartitions, maxIndexBufferSize, tempPath.toString(), writeOrder);
    }

    private PartitionedFileWriter createAndFinishPartitionedFileWriter() throws IOException {
        PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(1, new int[0]);
        partitionedFile = partitionedFileWriter.finish();
        return partitionedFileWriter;
    }

    public static ByteBuffer createAndConfigIndexEntryBuffer() {
        ByteBuffer indexEntryBuffer = ByteBuffer.allocateDirect(PartitionedFile.INDEX_ENTRY_SIZE);
        BufferReaderWriterUtil.configureByteBuffer(indexEntryBuffer);
        return indexEntryBuffer;
    }
}
