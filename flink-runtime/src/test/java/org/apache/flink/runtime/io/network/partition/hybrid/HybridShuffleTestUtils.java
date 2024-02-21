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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.util.TestCounter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndexImpl.InternalRegion;
import org.apache.flink.runtime.io.network.partition.hybrid.index.FileDataIndexRegionHelper;
import org.apache.flink.runtime.io.network.partition.hybrid.index.TestingFileDataIndexRegion;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.file.ProducerMergedPartitionFileIndex;
import org.apache.flink.runtime.metrics.TimerGauge;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import static org.apache.flink.runtime.io.network.partition.hybrid.index.TestingFileDataIndexRegion.getContainBufferFunction;
import static org.assertj.core.api.Assertions.assertThat;

/** Test utils for hybrid shuffle mode. */
public class HybridShuffleTestUtils {
    public static final int MEMORY_SEGMENT_SIZE = 128;

    public static List<BufferIndexAndChannel> createBufferIndexAndChannelsList(
            int subpartitionId, int... bufferIndexes) {
        List<BufferIndexAndChannel> bufferIndexAndChannels = new ArrayList<>();
        for (int bufferIndex : bufferIndexes) {
            bufferIndexAndChannels.add(new BufferIndexAndChannel(bufferIndex, subpartitionId));
        }
        return bufferIndexAndChannels;
    }

    public static Deque<BufferIndexAndChannel> createBufferIndexAndChannelsDeque(
            int subpartitionId, int... bufferIndexes) {
        Deque<BufferIndexAndChannel> bufferIndexAndChannels = new ArrayDeque<>();
        for (int bufferIndex : bufferIndexes) {
            bufferIndexAndChannels.add(new BufferIndexAndChannel(bufferIndex, subpartitionId));
        }
        return bufferIndexAndChannels;
    }

    public static Buffer createBuffer(int bufferSize, boolean isEvent) {
        return new NetworkBuffer(
                MemorySegmentFactory.allocateUnpooledSegment(bufferSize),
                FreeingBufferRecycler.INSTANCE,
                isEvent ? Buffer.DataType.EVENT_BUFFER : Buffer.DataType.DATA_BUFFER,
                bufferSize);
    }

    public static BufferBuilder createBufferBuilder(int bufferSize) {
        return new BufferBuilder(
                MemorySegmentFactory.allocateUnpooledSegment(bufferSize),
                FreeingBufferRecycler.INSTANCE);
    }

    public static HsOutputMetrics createTestingOutputMetrics() {
        return new HsOutputMetrics(new TestCounter(), new TestCounter(), new TimerGauge());
    }

    public static TestingFileDataIndexRegion createSingleTestRegion(
            int firstBufferIndex, long firstBufferOffset, int numBuffersPerRegion) {
        return new TestingFileDataIndexRegion.Builder()
                .setGetSizeSupplier(() -> TestingFileDataIndexRegion.REGION_SIZE)
                .setContainBufferFunction(
                        bufferIndex ->
                                getContainBufferFunction(
                                        bufferIndex, firstBufferIndex, numBuffersPerRegion))
                .setGetFirstBufferIndexSupplier(() -> firstBufferIndex)
                .setGetRegionFileOffsetSupplier(() -> firstBufferOffset)
                .setGetNumBuffersSupplier(() -> numBuffersPerRegion)
                .build();
    }

    public static List<TestingFileDataIndexRegion> createTestRegions(
            int firstBufferIndex, long firstBufferOffset, int numBuffersPerRegion, int numRegions) {
        List<TestingFileDataIndexRegion> regions = new ArrayList<>();
        int bufferIndex = firstBufferIndex;
        long bufferOffset = firstBufferOffset;
        int numRegionSize = TestingFileDataIndexRegion.REGION_SIZE;
        for (int i = 0; i < numRegions; i++) {
            final int currentBufferIndex = bufferIndex;
            final long currentBufferOffset = bufferOffset;
            regions.add(
                    new TestingFileDataIndexRegion.Builder()
                            .setGetSizeSupplier(() -> numRegionSize)
                            .setGetFirstBufferIndexSupplier(() -> currentBufferIndex)
                            .setGetRegionFileOffsetSupplier(() -> currentBufferOffset)
                            .setGetNumBuffersSupplier(() -> numBuffersPerRegion)
                            .setContainBufferFunction(
                                    index ->
                                            getContainBufferFunction(
                                                    index, firstBufferIndex, numBuffersPerRegion))
                            .build());
            bufferIndex += numBuffersPerRegion;
            bufferOffset += bufferOffset;
        }
        return regions;
    }

    public static FileDataIndexRegionHelper.Region createSingleFixedSizeRegion(
            int firstBufferIndex,
            long firstBufferOffset,
            long lastBufferEndOffset,
            int numBuffersPerRegion) {
        return new ProducerMergedPartitionFileIndex.FixedSizeRegion(
                firstBufferIndex, firstBufferOffset, lastBufferEndOffset, numBuffersPerRegion);
    }

    public static void assertRegionEquals(
            FileDataIndexRegionHelper.Region expected, FileDataIndexRegionHelper.Region region) {
        assertThat(region.getFirstBufferIndex()).isEqualTo(expected.getFirstBufferIndex());
        assertThat(region.getRegionStartOffset()).isEqualTo(expected.getRegionStartOffset());
        assertThat(region.getNumBuffers()).isEqualTo(expected.getNumBuffers());
        if (expected instanceof InternalRegion) {
            assertThat(region).isInstanceOf(InternalRegion.class);
            assertThat(((InternalRegion) region).getReleased())
                    .isEqualTo(((InternalRegion) expected).getReleased());
        }
    }
}
