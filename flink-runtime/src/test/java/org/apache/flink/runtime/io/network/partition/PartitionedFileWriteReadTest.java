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
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.IOUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for writing and reading {@link PartitionedFile} with {@link PartitionedFileWriter} and
 * {@link PartitionedFileReader}.
 */
public class PartitionedFileWriteReadTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testWriteAndReadPartitionedFile() throws Exception {
        int numSubpartitions = 10;
        int bufferSize = 1024;
        int numBuffers = 1000;
        int numRegions = 10;
        Random random = new Random(1111);

        List<Buffer>[] buffersWritten = new List[numSubpartitions];
        List<Buffer>[] buffersRead = new List[numSubpartitions];
        List<BufferWithChannel>[] regionBuffers = new List[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            buffersWritten[subpartition] = new ArrayList<>();
            buffersRead[subpartition] = new ArrayList<>();
            regionBuffers[subpartition] = new ArrayList<>();
        }

        PartitionedFileWriter fileWriter = createPartitionedFileWriter(numSubpartitions);
        for (int region = 0; region < numRegions; ++region) {
            boolean isBroadcastRegion = random.nextBoolean();
            fileWriter.startNewRegion(isBroadcastRegion);

            for (int i = 0; i < numBuffers; ++i) {
                Buffer buffer = createBuffer(random, bufferSize);
                if (isBroadcastRegion) {
                    for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                        buffersWritten[subpartition].add(buffer);
                        regionBuffers[subpartition].add(
                                new BufferWithChannel(buffer, subpartition));
                    }
                } else {
                    int subpartition = random.nextInt(numSubpartitions);
                    buffersWritten[subpartition].add(buffer);
                    regionBuffers[subpartition].add(new BufferWithChannel(buffer, subpartition));
                }
            }

            int[] writeOrder =
                    PartitionSortedBufferTest.getRandomSubpartitionOrder(numSubpartitions);
            for (int index = 0; index < numSubpartitions; ++index) {
                int subpartition = writeOrder[index];
                fileWriter.writeBuffers(regionBuffers[subpartition]);
                if (isBroadcastRegion) {
                    break;
                }
            }

            for (int index = 0; index < numSubpartitions; ++index) {
                regionBuffers[index].clear();
            }
        }
        PartitionedFile partitionedFile = fileWriter.finish();

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            PartitionedFileReader fileReader =
                    new PartitionedFileReader(
                            partitionedFile, subpartition, dataFileChannel, indexFileChannel);
            while (fileReader.hasRemaining()) {
                MemorySegment readBuffer = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
                Buffer buffer = fileReader.readCurrentRegion(readBuffer, (buf) -> {});
                buffersRead[subpartition].add(buffer);
            }
        }
        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);

        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            assertEquals(buffersWritten[subpartition].size(), buffersRead[subpartition].size());
            for (int i = 0; i < buffersWritten[subpartition].size(); ++i) {
                assertBufferEquals(
                        buffersWritten[subpartition].get(i), buffersRead[subpartition].get(i));
            }
        }
    }

    @Test
    public void testWriteAndReadWithEmptySubpartition() throws Exception {
        int numRegions = 10;
        int numSubpartitions = 5;
        int bufferSize = 1024;
        Random random = new Random(1111);

        Queue<Buffer>[] subpartitionBuffers = new ArrayDeque[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            subpartitionBuffers[subpartition] = new ArrayDeque<>();
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
                            partitionedFile, subpartition, dataFileChannel, indexFileChannel);
            while (fileReader.hasRemaining()) {
                MemorySegment readBuffer = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
                Buffer buffer = checkNotNull(fileReader.readCurrentRegion(readBuffer, (buf) -> {}));
                assertBufferEquals(checkNotNull(subpartitionBuffers[subpartition].poll()), buffer);
            }
            assertTrue(subpartitionBuffers[subpartition].isEmpty());
        }
        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);
    }

    private void assertBufferEquals(Buffer expected, Buffer actual) {
        assertEquals(expected.getDataType(), actual.getDataType());
        assertEquals(expected.getNioBufferReadable(), actual.getNioBufferReadable());
    }

    private Buffer createBuffer(Random random, int bufferSize) {
        boolean isBuffer = random.nextBoolean();
        Buffer.DataType dataType =
                isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;

        int dataSize = random.nextInt(bufferSize) + 1;
        byte[] data = new byte[dataSize];
        return new NetworkBuffer(MemorySegmentFactory.wrap(data), (buf) -> {}, dataType, dataSize);
    }

    @Test(expected = IllegalStateException.class)
    public void testNotWriteDataOfTheSameSubpartitionTogether() throws Exception {
        PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(2);
        try {
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);

            NetworkBuffer buffer1 = new NetworkBuffer(segment, (buf) -> {});
            partitionedFileWriter.writeBuffers(getBufferWithChannels(buffer1, 1));

            NetworkBuffer buffer2 = new NetworkBuffer(segment, (buf) -> {});
            partitionedFileWriter.writeBuffers(getBufferWithChannels(buffer2, 0));

            NetworkBuffer buffer3 = new NetworkBuffer(segment, (buf) -> {});
            partitionedFileWriter.writeBuffers(getBufferWithChannels(buffer3, 1));
        } finally {
            partitionedFileWriter.finish();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteFinishedPartitionedFile() throws Exception {
        PartitionedFileWriter partitionedFileWriter = createAndFinishPartitionedFileWriter();

        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
        NetworkBuffer buffer = new NetworkBuffer(segment, (buf) -> {});

        partitionedFileWriter.writeBuffers(getBufferWithChannels(buffer, 0));
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishPartitionedFileWriterTwice() throws Exception {
        PartitionedFileWriter partitionedFileWriter = createAndFinishPartitionedFileWriter();
        partitionedFileWriter.finish();
    }

    @Test
    public void testReadEmptyPartitionedFile() throws Exception {
        PartitionedFile partitionedFile = createPartitionedFile();

        FileChannel dataFileChannel = openFileChannel(partitionedFile.getDataFilePath());
        FileChannel indexFileChannel = openFileChannel(partitionedFile.getIndexFilePath());
        PartitionedFileReader partitionedFileReader =
                new PartitionedFileReader(partitionedFile, 1, dataFileChannel, indexFileChannel);
        MemorySegment target = MemorySegmentFactory.allocateUnpooledSegment(1024);

        assertNull(partitionedFileReader.readCurrentRegion(target, FreeingBufferRecycler.INSTANCE));
        IOUtils.closeAllQuietly(dataFileChannel, indexFileChannel);
    }

    private FileChannel openFileChannel(Path path) throws IOException {
        return FileChannel.open(path, StandardOpenOption.READ);
    }

    private List<BufferWithChannel> getBufferWithChannels(Buffer buffer, int channelIndex) {
        return Collections.singletonList(new BufferWithChannel(buffer, channelIndex));
    }

    private PartitionedFile createPartitionedFile() throws IOException {
        PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(2);
        return partitionedFileWriter.finish();
    }

    private PartitionedFileWriter createPartitionedFileWriter(int numSubpartitions)
            throws IOException {
        String basePath = temporaryFolder.newFile().getPath();
        return new PartitionedFileWriter(numSubpartitions, 640, basePath);
    }

    private PartitionedFileWriter createAndFinishPartitionedFileWriter() throws IOException {
        PartitionedFileWriter partitionedFileWriter = createPartitionedFileWriter(1);
        partitionedFileWriter.finish();
        return partitionedFileWriter;
    }
}
