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

import org.apache.flink.runtime.io.network.partition.BufferReaderWriterUtil;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageTestUtils.generateBuffersToWrite;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProducerMergedPartitionFileWriter}. */
class ProducerMergedPartitionFileWriterTest {

    @TempDir private Path tempFolder;

    @Test
    void testWrite() throws IOException {
        int numSubpartitions = 5;
        int numSegments = 10;
        int numBuffersPerSegment = 10;
        int bufferSizeBytes = 3;
        AtomicInteger receivedBuffers = new AtomicInteger(0);
        TestingProducerMergedPartitionFileIndex partitionFileIndex =
                new TestingProducerMergedPartitionFileIndex.Builder()
                        .setIndexFilePath(new File(tempFolder.toFile(), "testIndex").toPath())
                        .setAddBuffersConsumer(buffers -> receivedBuffers.getAndAdd(buffers.size()))
                        .build();

        Path testFile = new File(tempFolder.toFile(), "testFile").toPath();
        ProducerMergedPartitionFileWriter partitionFileWriter =
                new ProducerMergedPartitionFileWriter(testFile, partitionFileIndex);

        // Prepare the buffers to be written
        List<PartitionFileWriter.SubpartitionBufferContext> subpartitionBuffers =
                generateBuffersToWrite(
                        numSubpartitions, numSegments, numBuffersPerSegment, bufferSizeBytes);

        // Write the file
        partitionFileWriter.write(
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID()),
                subpartitionBuffers);
        partitionFileWriter.release();

        // Read the file
        byte[] bytesRead = Files.readAllBytes(testFile);

        int numExpectedBuffers = numSubpartitions * numSegments * numBuffersPerSegment;
        int numExpectedBytes =
                numExpectedBuffers * (BufferReaderWriterUtil.HEADER_LENGTH + bufferSizeBytes);
        assertThat(receivedBuffers).hasValue(numExpectedBuffers);
        assertThat(bytesRead.length).isEqualTo(numExpectedBytes);
    }

    @Test
    void testRelease() {
        AtomicBoolean isReleased = new AtomicBoolean(false);
        TestingProducerMergedPartitionFileIndex partitionFileIndex =
                new TestingProducerMergedPartitionFileIndex.Builder()
                        .setIndexFilePath(new File(tempFolder.toFile(), "testIndex").toPath())
                        .setReleaseRunnable(() -> isReleased.set(true))
                        .build();
        ProducerMergedPartitionFileWriter partitionFileWriter =
                new ProducerMergedPartitionFileWriter(
                        new File(tempFolder.toFile(), "testFile1").toPath(), partitionFileIndex);
        partitionFileWriter.release();
        assertThat(isReleased).isTrue();
    }
}
