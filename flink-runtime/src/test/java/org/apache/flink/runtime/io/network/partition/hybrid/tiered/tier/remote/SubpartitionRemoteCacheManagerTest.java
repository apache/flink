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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.TestingPartitionFileWriter;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SubpartitionRemoteCacheManager}. */
class SubpartitionRemoteCacheManagerTest {

    @Test
    void testStartAndFinishSegment() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;
        int segmentId = 0;

        AtomicInteger numReceivedBuffers = new AtomicInteger(0);
        TestingPartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setWriteFunction(
                                (ignoredPartitionId, bufferContexts) -> {
                                    numReceivedBuffers.addAndGet(
                                            bufferContexts
                                                    .get(subpartitionId)
                                                    .getSegmentBufferContexts()
                                                    .get(segmentId)
                                                    .getBufferAndIndexes()
                                                    .size());
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        SubpartitionRemoteCacheManager cacheManager =
                new SubpartitionRemoteCacheManager(
                        partitionId,
                        subpartitionId,
                        new TestingTieredStorageMemoryManager.Builder().build(),
                        partitionFileWriter);
        cacheManager.startSegment(segmentId);
        cacheManager.addBuffer(BufferBuilderTestUtils.buildSomeBuffer());
        cacheManager.finishSegment(segmentId);
        assertThat(numReceivedBuffers).hasValue(1);
    }

    @Test
    void testStartSegmentWithUnFlushedBuffers() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;
        int segmentId = 0;

        SubpartitionRemoteCacheManager cacheManager =
                new SubpartitionRemoteCacheManager(
                        partitionId,
                        subpartitionId,
                        new TestingTieredStorageMemoryManager.Builder().build(),
                        new TestingPartitionFileWriter.Builder().build());
        cacheManager.addBuffer(BufferBuilderTestUtils.buildSomeBuffer());
        assertThatThrownBy(() -> cacheManager.startSegment(segmentId))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testFinishWrongSegment() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;
        int segmentId = 0;

        SubpartitionRemoteCacheManager cacheManager =
                new SubpartitionRemoteCacheManager(
                        partitionId,
                        subpartitionId,
                        new TestingTieredStorageMemoryManager.Builder().build(),
                        new TestingPartitionFileWriter.Builder().build());
        cacheManager.startSegment(segmentId);
        assertThatThrownBy(() -> cacheManager.finishSegment(1))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRecycleBuffers() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;

        AtomicInteger numWrittenBuffers = new AtomicInteger(0);
        PartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setWriteFunction(
                                (ignore, bufferContexts) -> {
                                    for (PartitionFileWriter.SubpartitionBufferContext
                                            subpartitionBufferContext : bufferContexts) {
                                        for (PartitionFileWriter.SegmentBufferContext
                                                segmentBufferContext :
                                                        subpartitionBufferContext
                                                                .getSegmentBufferContexts()) {
                                            numWrittenBuffers.getAndAdd(
                                                    segmentBufferContext
                                                            .getBufferAndIndexes()
                                                            .size());
                                        }
                                    }
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        SubpartitionRemoteCacheManager cacheManager =
                new SubpartitionRemoteCacheManager(
                        partitionId,
                        subpartitionId,
                        new TestingTieredStorageMemoryManager.Builder().build(),
                        partitionFileWriter);
        Buffer buffer =
                new NetworkBuffer(
                        MemorySegmentFactory.allocateUnpooledSegment(1),
                        FreeingBufferRecycler.INSTANCE);
        cacheManager.addBuffer(buffer);
        cacheManager.close();
        cacheManager.release();
        assertThat(numWrittenBuffers).hasValue(1);
    }

    @Test
    void testClose() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;
        int segmentId = 0;

        AtomicInteger numReceivedBuffers = new AtomicInteger(0);
        TestingPartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setWriteFunction(
                                (ignoredPartitionId, bufferContexts) -> {
                                    numReceivedBuffers.addAndGet(
                                            bufferContexts
                                                    .get(subpartitionId)
                                                    .getSegmentBufferContexts()
                                                    .get(segmentId)
                                                    .getBufferAndIndexes()
                                                    .size());
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        SubpartitionRemoteCacheManager cacheManager =
                new SubpartitionRemoteCacheManager(
                        partitionId,
                        subpartitionId,
                        new TestingTieredStorageMemoryManager.Builder().build(),
                        partitionFileWriter);
        cacheManager.addBuffer(BufferBuilderTestUtils.buildSomeBuffer());
        cacheManager.close();
        assertThat(numReceivedBuffers).hasValue(1);
    }

    @Test
    void testRelease() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        int subpartitionId = 0;
        AtomicBoolean isReleased = new AtomicBoolean(false);
        TestingPartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setReleaseRunnable(() -> isReleased.set(true))
                        .build();
        SubpartitionRemoteCacheManager cacheManager =
                new SubpartitionRemoteCacheManager(
                        partitionId,
                        subpartitionId,
                        new TestingTieredStorageMemoryManager.Builder().build(),
                        partitionFileWriter);
        cacheManager.release();
        assertThat(isReleased).isTrue();
    }
}
