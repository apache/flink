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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory;

import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TestingNettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TestingNettyServiceProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TestingTieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MemoryTierProducerAgent}. */
class MemoryTierProducerAgentTest {

    private static final int NUM_SUBPARTITIONS = 10;

    private static final int BUFFER_SIZE = 1024;

    private static final int SEGMENT_SIZE_BYTES = BUFFER_SIZE * 2;

    private static final int MEMORY_TIER_SUBPARTITION_MAX_QUEUED_BUFFERS = 3;

    private static final TieredStoragePartitionId PARTITION_ID =
            TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
    private static final TieredStorageSubpartitionId SUBPARTITION_ID =
            new TieredStorageSubpartitionId(0);

    @Test
    void testTryStartNewSegment() {
        try (MemoryTierProducerAgent memoryTierProducerAgent =
                createMemoryTierProducerAgent(false)) {
            assertThat(memoryTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0)).isFalse();
            memoryTierProducerAgent.connectionEstablished(
                    SUBPARTITION_ID, new TestingNettyConnectionWriter.Builder().build());
            assertThat(memoryTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0)).isTrue();
        }
    }

    @Test
    void testStartSegmentSuccessWhenSubpartitionOccupyFewBuffers() {
        int numQueuedBuffers = MEMORY_TIER_SUBPARTITION_MAX_QUEUED_BUFFERS - 1;
        try (MemoryTierProducerAgent memoryTierProducerAgent =
                createMemoryTierProducerAgent(
                        false,
                        SEGMENT_SIZE_BYTES,
                        MEMORY_TIER_SUBPARTITION_MAX_QUEUED_BUFFERS,
                        new TieredStorageResourceRegistry())) {
            TestingNettyConnectionWriter connectionWriter =
                    new TestingNettyConnectionWriter.Builder()
                            .setNumQueuedBufferPayloadsSupplier(() -> numQueuedBuffers)
                            .build();
            memoryTierProducerAgent.connectionEstablished(SUBPARTITION_ID, connectionWriter);
            assertThat(memoryTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0)).isTrue();
        }
    }

    @Test
    void testStartSegmentFailedWhenSubpartitionOccupyTooManyBuffers() {
        int numQueuedBuffers = MEMORY_TIER_SUBPARTITION_MAX_QUEUED_BUFFERS;
        try (MemoryTierProducerAgent memoryTierProducerAgent =
                createMemoryTierProducerAgent(
                        false,
                        SEGMENT_SIZE_BYTES,
                        numQueuedBuffers,
                        new TieredStorageResourceRegistry())) {
            TestingNettyConnectionWriter connectionWriter =
                    new TestingNettyConnectionWriter.Builder()
                            .setNumQueuedBufferPayloadsSupplier(() -> numQueuedBuffers)
                            .build();
            memoryTierProducerAgent.connectionEstablished(SUBPARTITION_ID, connectionWriter);
            assertThat(memoryTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0)).isFalse();
        }
    }

    @Test
    void testStartSegmentFailedWithInsufficientMemory() {
        TestingTieredStorageMemoryManager memoryManager =
                new TestingTieredStorageMemoryManager.Builder()
                        .setGetMaxNonReclaimableBuffersFunction(ignore -> 1)
                        .build();
        TestingTieredStorageNettyService nettyService =
                new TestingTieredStorageNettyService.Builder().build();
        nettyService.registerProducer(
                PARTITION_ID, new TestingNettyServiceProducer.Builder().build());

        try (MemoryTierProducerAgent memoryTierProducerAgent =
                new MemoryTierProducerAgent(
                        PARTITION_ID,
                        NUM_SUBPARTITIONS,
                        BUFFER_SIZE,
                        SEGMENT_SIZE_BYTES,
                        MEMORY_TIER_SUBPARTITION_MAX_QUEUED_BUFFERS,
                        false,
                        memoryManager,
                        nettyService,
                        new TieredStorageResourceRegistry())) {
            memoryTierProducerAgent.connectionEstablished(
                    SUBPARTITION_ID, new TestingNettyConnectionWriter.Builder().build());
            assertThat(memoryTierProducerAgent.tryStartNewSegment(SUBPARTITION_ID, 0)).isFalse();
        }
    }

    @Test
    void testTryWrite() {
        try (MemoryTierProducerAgent memoryTierProducerAgent =
                createMemoryTierProducerAgent(
                        false, BUFFER_SIZE, new TieredStorageResourceRegistry())) {
            memoryTierProducerAgent.connectionEstablished(
                    SUBPARTITION_ID, new TestingNettyConnectionWriter.Builder().build());
            assertThat(
                            memoryTierProducerAgent.tryWrite(
                                    SUBPARTITION_ID,
                                    BufferBuilderTestUtils.buildSomeBuffer(),
                                    this))
                    .isTrue();
            assertThat(
                            memoryTierProducerAgent.tryWrite(
                                    SUBPARTITION_ID,
                                    BufferBuilderTestUtils.buildSomeBuffer(),
                                    this))
                    .isFalse();
        }
    }

    @Test
    void testBroadcastOnlyPartitionCanNotUseMemoryTier() {
        assertThatThrownBy(() -> createMemoryTierProducerAgent(true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not allowed to use the memory tier");
    }

    @Test
    void testRelease() {
        TieredStorageResourceRegistry resourceRegistry = new TieredStorageResourceRegistry();
        MemoryTierProducerAgent memoryTierProducerAgent =
                createMemoryTierProducerAgent(false, SEGMENT_SIZE_BYTES, resourceRegistry);

        AtomicBoolean isClosed = new AtomicBoolean(false);
        memoryTierProducerAgent.connectionEstablished(
                SUBPARTITION_ID,
                new TestingNettyConnectionWriter.Builder()
                        .setCloseFunction(
                                throwable -> {
                                    isClosed.set(true);
                                    return null;
                                })
                        .build());

        resourceRegistry.clearResourceFor(PARTITION_ID);
        assertThat(isClosed).isTrue();
    }

    private static MemoryTierProducerAgent createMemoryTierProducerAgent(boolean isBroadcastOnly) {
        return createMemoryTierProducerAgent(
                isBroadcastOnly, SEGMENT_SIZE_BYTES, new TieredStorageResourceRegistry());
    }

    private static MemoryTierProducerAgent createMemoryTierProducerAgent(
            boolean isBroadcastOnly,
            int segmentSizeBytes,
            TieredStorageResourceRegistry resourceRegistry) {
        return createMemoryTierProducerAgent(
                isBroadcastOnly,
                segmentSizeBytes,
                MEMORY_TIER_SUBPARTITION_MAX_QUEUED_BUFFERS,
                resourceRegistry);
    }

    private static MemoryTierProducerAgent createMemoryTierProducerAgent(
            boolean isBroadcastOnly,
            int segmentSizeBytes,
            int memoryTierSubpartitionMaxQueuedBuffers,
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

        return new MemoryTierProducerAgent(
                PARTITION_ID,
                NUM_SUBPARTITIONS,
                BUFFER_SIZE,
                segmentSizeBytes,
                memoryTierSubpartitionMaxQueuedBuffers,
                isBroadcastOnly,
                memoryManager,
                nettyService,
                resourceRegistry);
    }
}
