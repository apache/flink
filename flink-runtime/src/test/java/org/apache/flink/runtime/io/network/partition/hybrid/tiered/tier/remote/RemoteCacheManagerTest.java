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

import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.TestingPartitionFileWriter;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RemoteCacheManager}. */
class RemoteCacheManagerTest {

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
        RemoteCacheManager cacheManager =
                new RemoteCacheManager(
                        partitionId,
                        1,
                        new TestingTieredStorageMemoryManager.Builder().build(),
                        partitionFileWriter);
        cacheManager.startSegment(subpartitionId, segmentId);
        cacheManager.appendBuffer(BufferBuilderTestUtils.buildSomeBuffer(), subpartitionId);
        cacheManager.finishSegment(subpartitionId);
        assertThat(numReceivedBuffers).hasValue(1);
    }

    @Test
    void testRelease() {
        TieredStoragePartitionId partitionId =
                TieredStorageIdMappingUtils.convertId(new ResultPartitionID());
        AtomicBoolean isReleased = new AtomicBoolean(false);
        TestingPartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setReleaseRunnable(() -> isReleased.set(true))
                        .build();
        RemoteCacheManager cacheManager =
                new RemoteCacheManager(
                        partitionId,
                        1,
                        new TestingTieredStorageMemoryManager.Builder().build(),
                        partitionFileWriter);
        cacheManager.release();
        assertThat(isReleased).isTrue();
    }
}
