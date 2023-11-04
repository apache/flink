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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.PartitionFileWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.TestingPartitionFileWriter;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DiskCacheManager}. */
class DiskCacheManagerTest {

    @Test
    void testAppend() {
        int numAddBuffers = 100;
        int maxBufferSizeBytes = 100;
        int subpartitionId = 0;
        Random random = new Random();

        TestingTieredStorageMemoryManager memoryManager =
                new TestingTieredStorageMemoryManager.Builder().build();

        AtomicInteger numReceivedBuffers = new AtomicInteger(0);
        AtomicInteger numReceivedBytes = new AtomicInteger(0);
        TestingPartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setWriteFunction(
                                (partitionId, subpartitionBufferContexts) -> {
                                    Tuple2<Integer, Integer> numBuffersAndBytes =
                                            getNumReceivedBuffersAndBytes(
                                                    subpartitionBufferContexts);
                                    numReceivedBuffers.getAndAdd(numBuffersAndBytes.f0);
                                    numReceivedBytes.getAndAdd(numBuffersAndBytes.f1);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        TieredStorageIdMappingUtils.convertId(new ResultPartitionID()),
                        1,
                        1024,
                        memoryManager,
                        partitionFileWriter);

        // Append the buffers to the disk cache manager
        int numExpectBytes = 0;
        for (int i = 0; i < numAddBuffers; i++) {
            int bufferSizeBytes = random.nextInt(maxBufferSizeBytes) + 1;
            numExpectBytes += bufferSizeBytes;
            diskCacheManager.append(
                    BufferBuilderTestUtils.buildSomeBuffer(bufferSizeBytes), subpartitionId);
        }

        assertThat(diskCacheManager.getBufferIndex(subpartitionId)).isEqualTo(numAddBuffers);
        diskCacheManager.close();

        assertThat(numReceivedBuffers).hasValue(numAddBuffers);
        assertThat(numReceivedBytes).hasValue(numExpectBytes);
    }

    @Test
    void testAppendEndOfSegmentEvent() throws IOException {
        TestingTieredStorageMemoryManager memoryManager =
                new TestingTieredStorageMemoryManager.Builder().build();

        List<PartitionFileWriter.SubpartitionBufferContext> receivedBuffers = new ArrayList<>();
        TestingPartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setWriteFunction(
                                (partitionId, subpartitionBufferContexts) -> {
                                    receivedBuffers.addAll(subpartitionBufferContexts);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        TieredStorageIdMappingUtils.convertId(new ResultPartitionID()),
                        1,
                        1024,
                        memoryManager,
                        partitionFileWriter);
        diskCacheManager.appendEndOfSegmentEvent(
                EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE), 0);

        diskCacheManager.close();
        assertThat(receivedBuffers).hasSize(1);
        List<PartitionFileWriter.SegmentBufferContext> segmentBufferContexts =
                receivedBuffers.get(0).getSegmentBufferContexts();
        assertThat(segmentBufferContexts).hasSize(1);
        List<Tuple2<Buffer, Integer>> bufferAndIndexes =
                segmentBufferContexts.get(0).getBufferAndIndexes();
        assertThat(bufferAndIndexes).hasSize(1);
        Buffer buffer = bufferAndIndexes.get(0).f0;

        assertThat(buffer.isBuffer()).isFalse();
        AbstractEvent event =
                EventSerializer.fromSerializedEvent(
                        buffer.readOnlySlice().getNioBufferReadable(), getClass().getClassLoader());
        assertThat(event).isInstanceOf(EndOfSegmentEvent.class);
    }

    @Test
    void testFlushWhenCachedBytesReachLimit() throws IOException {
        TestingTieredStorageMemoryManager memoryManager =
                new TestingTieredStorageMemoryManager.Builder().build();

        AtomicInteger numWriteTimes = new AtomicInteger(0);
        TestingPartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setWriteFunction(
                                (partitionId, subpartitionBufferContexts) -> {
                                    numWriteTimes.incrementAndGet();
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        TieredStorageIdMappingUtils.convertId(new ResultPartitionID()),
                        1,
                        1024,
                        memoryManager,
                        partitionFileWriter);
        diskCacheManager.append(BufferBuilderTestUtils.buildSomeBuffer(1024), 0);
        assertThat(numWriteTimes).hasValue(0);
        diskCacheManager.append(BufferBuilderTestUtils.buildSomeBuffer(1), 0);
        assertThat(numWriteTimes).hasValue(1);

        diskCacheManager.append(BufferBuilderTestUtils.buildSomeBuffer(1024), 0);
        assertThat(numWriteTimes).hasValue(1);
        diskCacheManager.appendEndOfSegmentEvent(
                EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE), 0);
        assertThat(numWriteTimes).hasValue(2);
    }

    @Test
    void testRelease() {
        AtomicBoolean isReleased = new AtomicBoolean(false);
        TestingTieredStorageMemoryManager memoryManager =
                new TestingTieredStorageMemoryManager.Builder().build();
        TestingPartitionFileWriter partitionFileWriter =
                new TestingPartitionFileWriter.Builder()
                        .setReleaseRunnable(() -> isReleased.set(true))
                        .build();
        DiskCacheManager diskCacheManager =
                new DiskCacheManager(
                        TieredStorageIdMappingUtils.convertId(new ResultPartitionID()),
                        1,
                        1024,
                        memoryManager,
                        partitionFileWriter);
        diskCacheManager.release();
        assertThat(isReleased).isTrue();
    }

    private Tuple2<Integer, Integer> getNumReceivedBuffersAndBytes(
            List<PartitionFileWriter.SubpartitionBufferContext> subpartitionBufferContexts) {
        int numReceivedBuffers = 0;
        int numReceivedBytes = 0;
        for (PartitionFileWriter.SubpartitionBufferContext subpartitionBufferContext :
                subpartitionBufferContexts) {
            for (PartitionFileWriter.SegmentBufferContext segmentBufferContext :
                    subpartitionBufferContext.getSegmentBufferContexts()) {
                numReceivedBuffers += segmentBufferContext.getBufferAndIndexes().size();
                numReceivedBytes +=
                        segmentBufferContext.getBufferAndIndexes().stream()
                                .mapToInt(bufferAndIndex -> bufferAndIndex.f0.readableBytes())
                                .sum();
            }
        }
        return new Tuple2<>(numReceivedBuffers, numReceivedBytes);
    }
}
