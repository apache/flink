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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
        Queue<Buffer>[] regionBuffers = new Queue[numSubpartitions];
        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            buffersWritten[subpartition] = new ArrayList<>();
            buffersRead[subpartition] = new ArrayList<>();
            regionBuffers[subpartition] = new ArrayDeque<>();
        }

        PartitionedFileWriter fileWriter = createPartitionedFileWriter(numSubpartitions);
        for (int region = 0; region < numRegions; ++region) {
            fileWriter.startNewRegion();

            for (int i = 0; i < numBuffers; ++i) {
                Buffer buffer = createBuffer(random, bufferSize);

                int subpartition = random.nextInt(numSubpartitions);
                buffersWritten[subpartition].add(buffer);
                regionBuffers[subpartition].add(buffer);
            }

            int[] writeOrder =
                    PartitionSortedBufferTest.getRandomSubpartitionOrder(numSubpartitions);
            for (int index = 0; index < numSubpartitions; ++index) {
                int subpartition = writeOrder[index];
                while (!regionBuffers[subpartition].isEmpty()) {
                    fileWriter.writeBuffer(regionBuffers[subpartition].poll(), subpartition);
                }
            }
        }
        PartitionedFile partitionedFile = fileWriter.finish();

        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            PartitionedFileReader fileReader =
                    new PartitionedFileReader(partitionedFile, subpartition);
            while (fileReader.hasRemaining()) {
                MemorySegment readBuffer = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
                Buffer buffer = fileReader.readBuffer(readBuffer, (buf) -> {});
                buffersRead[subpartition].add(buffer);
            }
            fileReader.close();
        }

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
            fileWriter.startNewRegion();
            for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
                if (random.nextBoolean()) {
                    Buffer buffer = createBuffer(random, bufferSize);
                    subpartitionBuffers[subpartition].add(buffer);
                    fileWriter.writeBuffer(buffer, subpartition);
                }
            }
        }
        PartitionedFile partitionedFile = fileWriter.finish();

        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            PartitionedFileReader fileReader =
                    new PartitionedFileReader(partitionedFile, subpartition);
            while (fileReader.hasRemaining()) {
                MemorySegment readBuffer = MemorySegmentFactory.allocateUnpooledSegment(bufferSize);
                Buffer buffer = checkNotNull(fileReader.readBuffer(readBuffer, (buf) -> {}));
                assertBufferEquals(checkNotNull(subpartitionBuffers[subpartition].poll()), buffer);
            }
            fileReader.close();
            assertTrue(subpartitionBuffers[subpartition].isEmpty());
        }
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
            partitionedFileWriter.writeBuffer(buffer1, 1);

            NetworkBuffer buffer2 = new NetworkBuffer(segment, (buf) -> {});
            partitionedFileWriter.writeBuffer(buffer2, 0);

            NetworkBuffer buffer3 = new NetworkBuffer(segment, (buf) -> {});
            partitionedFileWriter.writeBuffer(buffer3, 1);
        } finally {
            partitionedFileWriter.finish();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteFinishedPartitionedFile() throws Exception {
        PartitionedFileWriter partitionedFileWriter = createAndFinishPartitionedFileWriter();

        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(1024);
        NetworkBuffer buffer = new NetworkBuffer(segment, (buf) -> {});

        partitionedFileWriter.writeBuffer(buffer, 0);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishPartitionedFileWriterTwice() throws Exception {
        PartitionedFileWriter partitionedFileWriter = createAndFinishPartitionedFileWriter();
        partitionedFileWriter.finish();
    }

    @Test(expected = IllegalStateException.class)
    public void testReadClosedPartitionedFile() throws Exception {
        PartitionedFileReader partitionedFileReader = createAndClosePartitionedFiledReader();

        MemorySegment target = MemorySegmentFactory.allocateUnpooledSegment(1024);
        partitionedFileReader.readBuffer(target, FreeingBufferRecycler.INSTANCE);
    }

    @Test
    public void testReadEmptyPartitionedFile() throws Exception {
        try (PartitionedFileReader partitionedFileReader = createPartitionedFiledReader()) {
            MemorySegment target = MemorySegmentFactory.allocateUnpooledSegment(1024);
            assertNull(partitionedFileReader.readBuffer(target, FreeingBufferRecycler.INSTANCE));
        }
    }

    private PartitionedFileReader createAndClosePartitionedFiledReader() throws IOException {
        PartitionedFileReader fileReader = createPartitionedFiledReader();
        fileReader.close();
        return fileReader;
    }

    private PartitionedFileReader createPartitionedFiledReader() throws IOException {
        PartitionedFile partitionedFile = createPartitionedFile();
        return new PartitionedFileReader(partitionedFile, 1);
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
