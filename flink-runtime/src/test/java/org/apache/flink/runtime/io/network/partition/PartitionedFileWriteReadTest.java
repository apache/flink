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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for writing and reading {@link PartitionedFile} with {@link PartitionedFileWriter} and
 * {@link PartitionedFileReader}.
 */
class PartitionedFileWriteReadTest {
    private @TempDir Path tempPath;

    @Test
    void testWriteAndReadPartitionedFile() throws Exception {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        int numBuffers = 1000;
        int numRegions = 10;

        List<Buffer>[] buffersWritten = new List[numSubpartitions];
        List<Buffer>[] buffersRead = new List[numSubpartitions];
        List<Tuple2<Long, Long>>[] regionStat = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            buffersWritten[subpartition] = new ArrayList<>();
            buffersRead[subpartition] = new ArrayList<>();
            regionStat[subpartition] = new ArrayList<>();
        }

        PartitionedFile partitionedFile =
                createPartitionedFile(
                        numSubpartitions,
                        bufferSize,
                        numBuffers,
                        numRegions,
                        buffersWritten,
                        regionStat,
                        createPartitionedFileWriter(numSubpartitions));

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            PartitionedFileReader fileReader =
                    new PartitionedFileReader(
                            partitionedFile,
                            subpartition,
                            dataFileChannel,
                            indexFileChannel,
                            BufferReaderWriterUtil.allocatedHeaderBuffer(),
                            createAndConfigIndexEntryBuffer());
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

    private PartitionedFile createPartitionedFile(
            int numSubpartitions,
            int bufferSize,
            int numBuffers,
            int numRegions,
            List<Buffer>[] buffersWritten,
            List<Tuple2<Long, Long>>[] regionStat,
            PartitionedFileWriter fileWriter)
            throws IOException {
        Random random = new Random(1111);
        long currentOffset = 0L;
        for (int region = 0; region < numRegions; ++region) {
            boolean isBroadcastRegion = random.nextBoolean();
            fileWriter.startNewRegion(isBroadcastRegion);
            List<BufferWithChannel>[] bufferWithChannels = new List[numSubpartitions];
            for (int i = 0; i < numSubpartitions; i++) {
                bufferWithChannels[i] = new ArrayList<>();
            }

            for (int i = 0; i < numBuffers; ++i) {
                Buffer buffer = createBuffer(random, bufferSize);
                if (isBroadcastRegion) {
                    for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                        buffersWritten[subpartition].add(buffer);
                        bufferWithChannels[subpartition].add(
                                new BufferWithChannel(buffer, subpartition));
                    }
                } else {
                    int subpartition = random.nextInt(numSubpartitions);
                    buffersWritten[subpartition].add(buffer);
                    bufferWithChannels[subpartition].add(
                            new BufferWithChannel(buffer, subpartition));
                }
            }

            int[] writeOrder = DataBufferTest.getRandomSubpartitionOrder(numSubpartitions);
            for (int index = 0; index < numSubpartitions; ++index) {
                int subpartition = writeOrder[index];
                fileWriter.writeBuffers(bufferWithChannels[subpartition]);
                long totalBytes = getTotalBytes(bufferWithChannels[subpartition]);
                if (isBroadcastRegion) {
                    for (int j = 0; j < numSubpartitions; j++) {
                        regionStat[j].add(Tuple2.of(currentOffset, totalBytes));
                    }
                    currentOffset += totalBytes;
                    break;
                } else {
                    regionStat[subpartition].add(Tuple2.of(currentOffset, totalBytes));
                    currentOffset += totalBytes;
                }
            }
        }
        return fileWriter.finish();
    }

    private static long getTotalBytes(List<BufferWithChannel> bufferWithChannels) {
        long totalBytes = 0L;
        for (BufferWithChannel bufferWithChannel : bufferWithChannels) {
            totalBytes +=
                    bufferWithChannel.getBuffer().readableBytes()
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
        int numBuffers = 2;
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

        PartitionedFileWriter fileWriter = createPartitionedFileWriter(numSubpartitions);
        for (int region = 0; region < numRegions; ++region) {
            fileWriter.startNewRegion(false);
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                if (random.nextBoolean()) {
                    Buffer buffer = createBuffer(random, bufferSize);
                    subpartitionBuffers[subpartition].add(buffer);
                    fileWriter.writeBuffers(getBufferWithChannels(buffer, subpartition));
                }
            }
        }
        PartitionedFile partitionedFile = fileWriter.finish();

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            PartitionedFileReader fileReader =
                    new PartitionedFileReader(
                            partitionedFile,
                            subpartition,
                            dataFileChannel,
                            indexFileChannel,
                            BufferReaderWriterUtil.allocatedHeaderBuffer(),
                            createAndConfigIndexEntryBuffer());
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
        PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(2);
        try {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);

            assertThatThrownBy(
                            () -> {
                                NetworkBuffer buffer1 = new NetworkBuffer(segment, (buf) -> {});
                                partitionedFileWriter.writeBuffers(
                                        getBufferWithChannels(buffer1, 1));

                                NetworkBuffer buffer2 = new NetworkBuffer(segment, (buf) -> {});
                                partitionedFileWriter.writeBuffers(
                                        getBufferWithChannels(buffer2, 0));

                                NetworkBuffer buffer3 = new NetworkBuffer(segment, (buf) -> {});
                                partitionedFileWriter.writeBuffers(
                                        getBufferWithChannels(buffer3, 1));
                            })
                    .isInstanceOf(IllegalStateException.class);

        } finally {
            partitionedFileWriter.finish();
        }
    }

    @Test
    void testWriteFinishedPartitionedFile() throws Exception {
        PartitionedFileWriter partitionedFileWriter = createAndFinishPartitionedFileWriter();

        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
        NetworkBuffer buffer = new NetworkBuffer(segment, (buf) -> {});

        assertThatThrownBy(
                        () -> partitionedFileWriter.writeBuffers(getBufferWithChannels(buffer, 0)))
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
        PartitionedFile partitionedFile = createEmptyPartitionedFile();

        List<Buffer>[] buffersRead = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            buffersRead[subpartition] = new ArrayList<>();
        }

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        PartitionedFileReader partitionedFileReader =
                new PartitionedFileReader(
                        partitionedFile,
                        1,
                        dataFileChannel,
                        indexFileChannel,
                        BufferReaderWriterUtil.allocatedHeaderBuffer(),
                        createAndConfigIndexEntryBuffer());

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

        List<Buffer>[] buffersWritten = new List[numSubpartitions];
        List<Buffer>[] buffersRead = new List[numSubpartitions];
        List<Tuple2<Long, Long>>[] regionStat = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            buffersWritten[subpartition] = new ArrayList<>();
            buffersRead[subpartition] = new ArrayList<>();
            regionStat[subpartition] = new ArrayList<>();
        }

        PartitionedFile partitionedFile =
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
                                PartitionedFile.INDEX_ENTRY_SIZE * numSubpartitions));

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

    private List<BufferWithChannel> getBufferWithChannels(Buffer buffer, int channelIndex) {
        return Collections.singletonList(new BufferWithChannel(buffer, channelIndex));
    }

    private PartitionedFile createEmptyPartitionedFile() throws IOException {
        PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(2);
        return partitionedFileWriter.finish();
    }

    private PartitionedFileWriter createPartitionedFileWriter(int numSubpartitions)
            throws IOException {
        return createPartitionedFileWriter(numSubpartitions, 640);
    }

    private PartitionedFileWriter createPartitionedFileWriter(
            int numSubpartitions, int minIndexBufferSize, int maxIndexBufferSize)
            throws IOException {
        return new PartitionedFileWriter(
                numSubpartitions, minIndexBufferSize, maxIndexBufferSize, tempPath.toString());
    }

    private PartitionedFileWriter createPartitionedFileWriter(
            int numSubpartitions, int maxIndexBufferSize) throws IOException {
        return new PartitionedFileWriter(numSubpartitions, maxIndexBufferSize, tempPath.toString());
    }

    private PartitionedFileWriter createAndFinishPartitionedFileWriter() throws IOException {
        PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(1);
        partitionedFileWriter.finish();
        return partitionedFileWriter;
    }

    public static ByteBuffer createAndConfigIndexEntryBuffer() {
        ByteBuffer indexEntryBuffer = ByteBuffer.allocateDirect(PartitionedFile.INDEX_ENTRY_SIZE);
        BufferReaderWriterUtil.configureByteBuffer(indexEntryBuffer);
        return indexEntryBuffer;
    }
}
