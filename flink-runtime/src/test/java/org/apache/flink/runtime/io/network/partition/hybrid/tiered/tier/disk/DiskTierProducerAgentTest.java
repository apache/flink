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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk;

import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.TestingPartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.TestingPartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TestingNettyServiceProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TestingTieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFile.DATA_FILE_SUFFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DiskTierProducerAgent}. */
public class DiskTierProducerAgentTest {

    @TempDir private Path tempFolder;

    private static final int NUM_SUBPARTITIONS = 10;

    private static final int BUFFER_SIZE_BYTES = 1024;

    private static final int NUM_BYTES_PER_SEGMENT = BUFFER_SIZE_BYTES * 2;

    private static final TieredStoragePartitionId PARTITION_ID =
            TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
    private static final TieredStorageSubpartitionId SUBPARTITION_ID =
            new TieredStorageSubpartitionId(0);

    @Test
    void testStartNewSegmentSuccess() throws IOException {
        String partitionFile = TempDirUtils.newFile(tempFolder, "test").toString();
        File testFile = new File(partitionFile + DATA_FILE_SUFFIX);
        assertThat(testFile.createNewFile()).isTrue();
        try (DiskTierProducerAgent diskTierProducerAgent =
                createDiskTierProducerAgent(
                        false,
                        NUM_BYTES_PER_SEGMENT,
                        0,
                        partitionFile,
                        new TestingPartitionFileWriter.Builder().build(),
                        new TieredStorageResourceRegistry())) {
            assertThat(diskTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0)).isTrue();
        }
    }

    @Test
    void testStartNewSegmentFailed() {
        try (DiskTierProducerAgent diskTierProducerAgent =
                createDiskTierProducerAgent(
                        false,
                        NUM_BYTES_PER_SEGMENT,
                        1,
                        tempFolder.toString(),
                        new TestingPartitionFileWriter.Builder().build(),
                        new TieredStorageResourceRegistry())) {
            assertThat(diskTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0)).isFalse();
        }
    }

    @Test
    void testWriteSuccess() {
        try (DiskTierProducerAgent diskTierProducerAgent =
                createDiskTierProducerAgent(
                        false,
                        BUFFER_SIZE_BYTES,
                        0,
                        tempFolder.toString(),
                        new TestingPartitionFileWriter.Builder().build(),
                        new TieredStorageResourceRegistry())) {
            diskTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0);
            assertThat(
                            diskTierProducerAgent.tryWrite(
                                    SUBPARTITION_ID,
                                    BufferBuilderTestUtils.buildSomeBuffer(),
                                    this))
                    .isTrue();
        }
    }

    @Test
    void testWriteFailed() {
        try (DiskTierProducerAgent diskTierProducerAgent =
                createDiskTierProducerAgent(
                        false,
                        BUFFER_SIZE_BYTES,
                        0,
                        tempFolder.toString(),
                        new TestingPartitionFileWriter.Builder().build(),
                        new TieredStorageResourceRegistry())) {
            diskTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0);
            assertThat(
                            diskTierProducerAgent.tryWrite(
                                    SUBPARTITION_ID,
                                    BufferBuilderTestUtils.buildSomeBuffer(),
                                    this))
                    .isTrue();
            assertThat(
                            diskTierProducerAgent.tryWrite(
                                    SUBPARTITION_ID,
                                    BufferBuilderTestUtils.buildSomeBuffer(BUFFER_SIZE_BYTES),
                                    this))
                    .isFalse();
        }
    }

    @Test
    void testWriteBroadcastBuffer() {
        try (DiskTierProducerAgent diskTierProducerAgent =
                createDiskTierProducerAgent(
                        true,
                        NUM_BYTES_PER_SEGMENT,
                        0,
                        tempFolder.toString(),
                        new TestingPartitionFileWriter.Builder().build(),
                        new TieredStorageResourceRegistry())) {
            diskTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0);
            assertThatThrownBy(
                            () ->
                                    diskTierProducerAgent.tryWrite(
                                            new TieredStorageSubpartitionId(1),
                                            BufferBuilderTestUtils.buildSomeBuffer(),
                                            this))
                    .isInstanceOf(ArrayIndexOutOfBoundsException.class);
            assertThat(
                            diskTierProducerAgent.tryWrite(
                                    SUBPARTITION_ID,
                                    BufferBuilderTestUtils.buildSomeBuffer(),
                                    this))
                    .isTrue();
        }
    }

    @Test
    void testRelease() {
        AtomicBoolean isReleased = new AtomicBoolean(false);
        TestingPartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setReleaseRunnable(() -> isReleased.set(true))
                        .build();
        TieredStorageResourceRegistry resourceRegistry = new TieredStorageResourceRegistry();

        DiskTierProducerAgent diskTierProducerAgent =
                createDiskTierProducerAgent(
                        false,
                        NUM_BYTES_PER_SEGMENT,
                        0,
                        tempFolder.toString(),
                        partitionFileWriter,
                        resourceRegistry);
        diskTierProducerAgent.close();
        resourceRegistry.clearResourceFor(PARTITION_ID);
        assertThat(isReleased).isTrue();
    }

    private static DiskTierProducerAgent createDiskTierProducerAgent(
            boolean isBroadcastOnly,
            int numBytesPerSegment,
            float minReservedDiskSpaceFraction,
            String dataFileBasePath,
            PartitionFileWriter partitionFileWriter,
            TieredStorageResourceRegistry resourceRegistry) {
        TestingTieredStorageMemoryManager memoryManager =
                new TestingTieredStorageMemoryManager.Builder()
                        .setGetMaxNonReclaimableBuffersFunction(ignore -> Integer.MAX_VALUE)
                        .build();
        TestingTieredStorageNettyService nettyService =
                new TestingTieredStorageNettyService.Builder().build();
        TestingNettyServiceProducer nettyServiceProducer =
                new TestingNettyServiceProducer.Builder().build();
        nettyService.registerProducer(PARTITION_ID, nettyServiceProducer);
        Path dataFilePath = new File(dataFileBasePath + DATA_FILE_SUFFIX).toPath();

        return new DiskTierProducerAgent(
                PARTITION_ID,
                NUM_SUBPARTITIONS,
                numBytesPerSegment,
                BUFFER_SIZE_BYTES,
                BUFFER_SIZE_BYTES,
                dataFilePath,
                minReservedDiskSpaceFraction,
                isBroadcastOnly,
                partitionFileWriter,
                new TestingPartitionFileReader.Builder().build(),
                memoryManager,
                nettyService,
                resourceRegistry,
                new BatchShuffleReadBufferPool(1, 1),
                new ManuallyTriggeredScheduledExecutorService(),
                0,
                Duration.ofMinutes(5),
                0);
    }
}
