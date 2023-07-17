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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileReader;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFile;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.SegmentPartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TestingTierFactory;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.disk.DiskTierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory.MemoryTierConsumerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteStorageScanner;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.remote.RemoteTierConsumerAgent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageTestUtils.generateBuffersToWrite;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TieredStorageConsumerClient}. */
class TieredStorageConsumerClientTest {

    private static final TieredStoragePartitionId DEFAULT_PARTITION_ID =
            TieredStorageIdMappingUtils.convertId(new ResultPartitionID());

    private static final TieredStorageSubpartitionId DEFAULT_SUBPARTITION_ID =
            new TieredStorageSubpartitionId(0);

    private static final int DEFAULT_NUM_SUBPARTITION = 1;

    private static final int DEFAULT_SEGMENT_NUM = 2;

    private static final int DEFAULT_BUFFER_PER_SEGMENT = 3;

    private static final int DEFAULT_BUFFER_SIZE = 1;

    @TempDir private java.nio.file.Path tempFolder;

    @Test
    void testGetNextBufferFromMemoryTier() {
        Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer(0);
        Optional<Buffer> nextBuffer =
                createTieredStorageConsumerClient(segmentId -> buffer, MemoryTierConsumerAgent::new)
                        .getNextBuffer(DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID);
        assertThat(nextBuffer).hasValue(buffer);
    }

    @Test
    void testGetNextBufferFromDiskTier() {
        Buffer buffer = BufferBuilderTestUtils.buildSomeBuffer(0);
        Optional<Buffer> nextBuffer =
                createTieredStorageConsumerClient(segmentId -> buffer, DiskTierConsumerAgent::new)
                        .getNextBuffer(DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID);
        assertThat(nextBuffer).hasValue(buffer);
    }

    @Test
    void testGetNextBufferFromRemoteTier() {
        Path tieredStorageDir = Path.fromLocalFile(tempFolder.toFile());
        SegmentPartitionFileWriter partitionFileWriter =
                SegmentPartitionFile.createPartitionFileWriter(
                        tieredStorageDir.getPath(), DEFAULT_NUM_SUBPARTITION);
        List<PartitionFileWriter.SubpartitionBufferContext> subpartitionBuffers =
                generateBuffersToWrite(
                        DEFAULT_NUM_SUBPARTITION,
                        DEFAULT_SEGMENT_NUM,
                        DEFAULT_BUFFER_PER_SEGMENT,
                        DEFAULT_BUFFER_SIZE);
        partitionFileWriter.write(DEFAULT_PARTITION_ID, subpartitionBuffers);
        TieredStorageConsumerClient tieredStorageConsumerClient =
                createTieredStorageConsumerClient(
                        segmentId -> null,
                        (tieredStorageConsumerSpecs, nettyService) -> {
                            PartitionFileReader partitionFileReader =
                                    SegmentPartitionFile.createPartitionFileReader(
                                            tieredStorageDir.getPath());
                            RemoteStorageScanner remoteStorageScanner =
                                    new RemoteStorageScanner(tieredStorageDir.getPath());
                            RemoteTierConsumerAgent remoteTierConsumerAgent =
                                    new RemoteTierConsumerAgent(
                                            remoteStorageScanner,
                                            partitionFileReader,
                                            DEFAULT_BUFFER_SIZE);
                            remoteTierConsumerAgent.registerAvailabilityNotifier(
                                    (partitionId, subpartitionId) -> {});
                            return remoteTierConsumerAgent;
                        });
        int totalBufferNumber = DEFAULT_SEGMENT_NUM * DEFAULT_BUFFER_PER_SEGMENT;
        while (totalBufferNumber-- > 0) {
            assertThat(
                            tieredStorageConsumerClient.getNextBuffer(
                                    DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID))
                    .isPresent();
        }
        assertThat(
                        tieredStorageConsumerClient.getNextBuffer(
                                DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID))
                .isNotPresent();
    }

    private TieredStorageConsumerClient createTieredStorageConsumerClient(
            Function<Integer, Buffer> readBufferFunction,
            BiFunction<
                            List<TieredStorageConsumerSpec>,
                            TieredStorageNettyService,
                            TierConsumerAgent>
                    tierConsumerAgentSupplier) {

        TestingTieredStorageNettyService nettyService =
                new TestingTieredStorageNettyService.Builder()
                        .setRegisterConsumerFunction(
                                (partitionId, subpartitionId) ->
                                        CompletableFuture.completedFuture(
                                                new TestingNettyConnectionReader.Builder()
                                                        .setReadBufferFunction(readBufferFunction)
                                                        .build()))
                        .build();
        return new TieredStorageConsumerClient(
                Collections.singletonList(
                        new TestingTierFactory.Builder()
                                .setTierConsumerAgentSupplier(tierConsumerAgentSupplier)
                                .build()),
                Collections.singletonList(
                        new TieredStorageConsumerSpec(
                                DEFAULT_PARTITION_ID, DEFAULT_SUBPARTITION_ID)),
                nettyService);
    }
}
