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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.util.Random;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.SEGMENT_FILE_PREFIX;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.SEGMENT_FINISH_DIR_NAME;
import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile.TIERED_STORAGE_DIR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SegmentPartitionFile}. */
class SegmentPartitionFileTest {

    @TempDir File tempFolder;

    @Test
    void testGetTieredStoragePath() {
        String tieredStoragePath = SegmentPartitionFile.getTieredStoragePath(tempFolder.getPath());
        assertThat(tieredStoragePath)
                .isEqualTo(new File(tempFolder.getPath(), TIERED_STORAGE_DIR).getPath());
    }

    @Test
    void testGetPartitionPath() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        String partitionPath =
                SegmentPartitionFile.getPartitionPath(partitionId, tempFolder.getPath());

        File partitionFile =
                new File(
                        tempFolder.getPath(),
                        TieredStorageIdMappingUtils.convertId(partitionId).toString());
        assertThat(partitionPath).isEqualTo(partitionFile.getPath());
    }

    @Test
    void testGetSubpartitionPath() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;

        String subpartitionPath =
                SegmentPartitionFile.getSubpartitionPath(
                        tempFolder.getPath(), partitionId, subpartitionId);
        File partitionFile =
                new File(
                        tempFolder.getPath(),
                        TieredStorageIdMappingUtils.convertId(partitionId).toString());
        assertThat(subpartitionPath)
                .isEqualTo(new File(partitionFile, String.valueOf(subpartitionId)).toString());
    }

    @Test
    void testGetSegmentPath() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;
        int segmentId = 1;

        String segmentPath =
                SegmentPartitionFile.getSegmentPath(
                                tempFolder.getPath(), partitionId, subpartitionId, segmentId)
                        .toString();

        File partitionFile =
                new File(
                        tempFolder.getPath(),
                        TieredStorageIdMappingUtils.convertId(partitionId).toString());
        File subpartitionFile = new File(partitionFile, String.valueOf(subpartitionId));
        assertThat(segmentPath)
                .isEqualTo(new File(subpartitionFile, SEGMENT_FILE_PREFIX + segmentId).toString());
    }

    @Test
    void testGetSegmentFinishDirPath() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;

        String segmentFinishDirPath =
                SegmentPartitionFile.getSegmentFinishDirPath(
                                tempFolder.getPath(), partitionId, subpartitionId)
                        .getPath();
        File expectedSegmentFinishDir =
                getSegmentFinishDir(tempFolder.getPath(), partitionId, subpartitionId);
        assertThat(segmentFinishDirPath).isEqualTo(expectedSegmentFinishDir.getPath());
    }

    @Test
    void testWriteBuffers() throws IOException {
        Random random = new Random();
        int numBuffers = 20;
        int bufferSizeBytes = 10;

        File testFile = new File(tempFolder.getPath(), "testFile");
        org.apache.flink.core.fs.Path testPath =
                org.apache.flink.core.fs.Path.fromLocalFile(testFile);
        FileSystem fs = testPath.getFileSystem();
        WritableByteChannel currentChannel =
                Channels.newChannel(fs.create(testPath, FileSystem.WriteMode.NO_OVERWRITE));

        ByteBuffer[] toWriteBuffers = new ByteBuffer[numBuffers];
        for (int i = 0; i < numBuffers; i++) {
            byte[] bytes = new byte[bufferSizeBytes];
            random.nextBytes(bytes);
            toWriteBuffers[i] = ByteBuffer.wrap(bytes);
        }
        int numExpectedBytes = numBuffers * bufferSizeBytes;
        SegmentPartitionFile.writeBuffers(currentChannel, numExpectedBytes, toWriteBuffers);

        byte[] bytesRead = Files.readAllBytes(testFile.toPath());
        assertThat(bytesRead).hasSize(numExpectedBytes);
    }

    @Test
    void testWriteSegmentFinishFile() throws IOException {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;
        int segmentId = 1;
        int newSegmentId = 5;

        writeAndCheckSegmentFinishFile(
                tempFolder.getPath(), partitionId, subpartitionId, segmentId);
        writeAndCheckSegmentFinishFile(
                tempFolder.getPath(), partitionId, subpartitionId, newSegmentId);
    }

    @Test
    void testDeletePathQuietly() throws IOException {
        File testFile = new File(tempFolder.getPath(), "testFile");
        Files.createFile(testFile.toPath());
        assertThat(testFile).exists();
        SegmentPartitionFile.deletePathQuietly(testFile.getPath());
        assertThat(testFile).doesNotExist();
    }

    private static void writeAndCheckSegmentFinishFile(
            String baseDir, TieredStoragePartitionId partitionId, int subpartitionId, int segmentId)
            throws IOException {

        SegmentPartitionFile.writeSegmentFinishFile(
                baseDir, partitionId, subpartitionId, segmentId);
        File segmentFinishDir = getSegmentFinishDir(baseDir, partitionId, subpartitionId);
        assertThat(segmentFinishDir.isDirectory()).isTrue();
        File[] segmentFinishFiles = segmentFinishDir.listFiles();
        assertThat(segmentFinishFiles).hasSize(1);
        assertThat(segmentFinishFiles[0]).isFile();
        assertThat(segmentFinishFiles[0].getName()).isEqualTo(String.valueOf(segmentId));
    }

    private static File getSegmentFinishDir(
            String baseDir, TieredStoragePartitionId partitionId, int subpartitionId) {
        File partitionFile =
                new File(baseDir, TieredStorageIdMappingUtils.convertId(partitionId).toString());
        File subpartitionFile = new File(partitionFile, String.valueOf(subpartitionId));
        return new File(subpartitionFile, SEGMENT_FINISH_DIR_NAME);
    }
}
