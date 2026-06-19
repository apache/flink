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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote;

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteTierProducerAgent}. */
class RemoteTierProducerAgentTest {

    @Test
    void test(@TempDir Path tempDir) throws Exception {
        final TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        final int numSubpartitions = 10;
        final int bufferSizeBytes = 1024;
        final int networkBufferSize = 1024;
        final int numBytesPerSegment = 4 * bufferSizeBytes;

        final PartitionFileWriter partitionFileWriter =
                SegmentPartitionFile.createPartitionFileWriter(
                        String.format("file://%s", tempDir), numSubpartitions);
        final TieredStorageMemoryManager memoryManager =
                new TestingTieredStorageMemoryManager.Builder().build();
        final TieredStorageResourceRegistry resourceRegistry = new TieredStorageResourceRegistry();
        final BufferCompressor bufferCompressor =
                new BufferCompressor(
                        networkBufferSize, NettyShuffleEnvironmentOptions.CompressionCodec.LZ4);

        try (final RemoteTierProducerAgent agent =
                new RemoteTierProducerAgent(
                        partitionId,
                        numSubpartitions,
                        numBytesPerSegment,
                        bufferSizeBytes,
                        false,
                        partitionFileWriter,
                        memoryManager,
                        resourceRegistry,
                        bufferCompressor)) {
            final Object bufferOwner = new Object();
            for (int i = 0; i < numSubpartitions; i += 2) {
                final TieredStorageSubpartitionId subpartitionId =
                        new TieredStorageSubpartitionId(i);
                assertThat(agent.tryStartNewSegment(subpartitionId, 0, 1)).isTrue();
                assertThat(
                                agent.tryWrite(
                                        subpartitionId,
                                        BufferBuilderTestUtils.buildSomeBuffer(bufferSizeBytes),
                                        bufferOwner,
                                        0))
                        .isTrue();
            }
        }

        try (final Stream<Path> files = Files.walk(tempDir)) {
            final long numFinishedSegments =
                    files.filter(Files::isRegularFile)
                            .filter(
                                    file ->
                                            SegmentPartitionFile.SEGMENT_FINISH_DIR_NAME.equals(
                                                    file.getParent().getFileName().toString()))
                            .count();
            assertThat(numFinishedSegments)
                    .withFailMessage("Only every second partition should have a finished segment.")
                    .isEqualTo(numSubpartitions / 2);
        }
    }
}
