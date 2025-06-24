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

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageTestUtils.generateBuffersToWrite;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.SEGMENT_FILE_PREFIX;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.SEGMENT_FINISH_DIR_NAME;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SegmentPartitionFileWriter}. */
class SegmentPartitionFileWriterTest {

    @TempDir java.nio.file.Path tempFolder;

    @Test
    void testWrite() throws IOException {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int numSubpartitions = 5;
        int numSegments = 10;
        int numBuffersPerSegment = 10;
        int bufferSizeBytes = 3;

        Path tieredStorageDir = Path.fromLocalFile(tempFolder.toFile());

        SegmentPartitionFileWriter partitionFileWriter =
                new SegmentPartitionFileWriter(tieredStorageDir.getPath(), numSubpartitions);

        // Prepare the buffers to be written
        List<PartitionFileWriter.SubpartitionBufferContext> subpartitionBuffers =
                generateBuffersToWrite(
                        numSubpartitions, numSegments, numBuffersPerSegment, bufferSizeBytes);

        // Write the file
        partitionFileWriter.write(partitionId, subpartitionBuffers);
        partitionFileWriter.release();

        // Check the written files
        checkWrittenSegmentFiles(
                partitionId,
                numSubpartitions,
                numSegments,
                numBuffersPerSegment,
                bufferSizeBytes,
                tieredStorageDir);
    }

    private static void checkWrittenSegmentFiles(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int numSegments,
            int numBuffersPerSegment,
            int bufferSizeBytes,
            Path tieredStorageDir)
            throws IOException {
        FileSystem fs = tieredStorageDir.getFileSystem();
        FileStatus[] partitionDirs = fs.listStatus(tieredStorageDir);
        assertThat(partitionDirs).hasSize(1);
        assertThat(partitionDirs[0].getPath().getName())
                .isEqualTo(TieredStorageIdMappingUtils.convertId(partitionId).toString());

        FileStatus[] subpartitionDirs = fs.listStatus(partitionDirs[0].getPath());
        assertThat(subpartitionDirs).hasSize(numSubpartitions);

        int expectedSegmentFileBytes =
                (BufferReaderWriterUtil.HEADER_LENGTH + bufferSizeBytes) * numBuffersPerSegment;
        for (int i = 0; i < numSubpartitions; i++) {
            Path subpartitionDir = subpartitionDirs[i].getPath();
            for (int j = 0; j < numSegments; j++) {
                Path segmentFile = new Path(subpartitionDir, SEGMENT_FILE_PREFIX + j);
                byte[] bytesRead =
                        Files.readAllBytes(new java.io.File(segmentFile.getPath()).toPath());
                // Check the segment file
                assertThat(bytesRead).hasSize(expectedSegmentFileBytes);
            }
            Path segmentFinishDir = new Path(subpartitionDir, SEGMENT_FINISH_DIR_NAME);
            assertThat(fs.exists(segmentFinishDir)).isTrue();
            // Check the segment-finish file
            FileStatus[] segmentFinishFiles = fs.listStatus(segmentFinishDir);
            assertThat(segmentFinishFiles).hasSize(1);
            FileStatus segmentFinishFile = segmentFinishFiles[0];
            assertThat(segmentFinishFile.getPath().getName())
                    .isEqualTo(String.valueOf(numSegments - 1));
        }
    }
}
