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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageTestUtils.generateBuffersToWrite;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SegmentPartitionFileReader}. */
class SegmentPartitionFileReaderTest {

    private static final TieredStoragePartitionId DEFAULT_PARTITION_ID =
            TieredStorageIdMappingUtils.convertId(new ResultPartitionID());

    private static final TieredStorageSubpartitionId DEFAULT_SUBPARTITION_ID =
            new TieredStorageSubpartitionId(0);

    private static final int DEFAULT_NUM_SUBPARTITION = 2;

    private static final int DEFAULT_SEGMENT_NUM = 2;

    private static final int DEFAULT_BUFFER_PER_SEGMENT = 3;

    private static final int DEFAULT_BUFFER_SIZE = 1;

    @TempDir private File tempFolder;

    private SegmentPartitionFileReader partitionFileReader;

    @BeforeEach
    void before() {
        Path tieredStorageDir = Path.fromLocalFile(tempFolder);
        SegmentPartitionFileWriter partitionFileWriter =
                new SegmentPartitionFileWriter(
                        tieredStorageDir.getPath(), DEFAULT_NUM_SUBPARTITION);

        // Prepare the buffers to be written.
        List<PartitionFileWriter.SubpartitionBufferContext> subpartitionBuffers =
                generateBuffersToWrite(
                        DEFAULT_NUM_SUBPARTITION,
                        DEFAULT_SEGMENT_NUM,
                        DEFAULT_BUFFER_PER_SEGMENT,
                        DEFAULT_BUFFER_SIZE);

        // Write the file.
        partitionFileWriter.write(DEFAULT_PARTITION_ID, subpartitionBuffers);
        partitionFileWriter.release();
        partitionFileReader = new SegmentPartitionFileReader(tieredStorageDir.getPath());
    }

    @Test
    void testReadBuffer() throws IOException {
        for (int subpartitionId = 0; subpartitionId < DEFAULT_NUM_SUBPARTITION; ++subpartitionId) {
            for (int segmentId = 0; segmentId < DEFAULT_SEGMENT_NUM; ++segmentId) {
                for (int bufferIndex = 0; bufferIndex < DEFAULT_BUFFER_PER_SEGMENT; ++bufferIndex) {
                    Buffer buffer =
                            readBuffer(
                                    bufferIndex,
                                    new TieredStorageSubpartitionId(subpartitionId),
                                    segmentId);
                    assertThat(buffer).isNotNull();
                    buffer.recycleBuffer();
                }
            }
        }
    }

    @Test
    void testGetPriority() throws IOException {
        assertThat(
                        partitionFileReader.getPriority(
                                DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID, 0, 0, null))
                .isEqualTo(-1);
        assertThat(readBuffer(0, DEFAULT_SUBPARTITION_ID, 0)).isNotNull();
        assertThat(
                        partitionFileReader.getPriority(
                                DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID, 0, 1, null))
                .isEqualTo(-1);
    }

    private Buffer readBuffer(
            int bufferIndex, TieredStorageSubpartitionId subpartitionId, int segmentId)
            throws IOException {
        MemorySegment memorySegment =
                MemorySegmentFactory.allocateUnpooledSegment(DEFAULT_BUFFER_SIZE);
        PartitionFileReader.ReadBufferResult readBufferResult =
                partitionFileReader.readBuffer(
                        DEFAULT_PARTITION_ID,
                        subpartitionId,
                        segmentId,
                        bufferIndex,
                        memorySegment,
                        FreeingBufferRecycler.INSTANCE,
                        null,
                        null);
        if (readBufferResult == null) {
            return null;
        }
        return readBufferResult.getReadBuffers().get(0);
    }
}
