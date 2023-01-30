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

import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndexImpl.InternalRegion;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.assertRegionEquals;
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createAllUnreleasedRegions;
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createSingleUnreleasedRegion;
import static org.apache.flink.runtime.io.network.partition.hybrid.InternalRegionWriteReadUtils.allocateAndConfigureBuffer;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsFileDataIndexSpilledRegionManagerImpl}. */
class HsFileDataIndexSpilledRegionManagerImplTest {
    private Path indexFilePath;

    @BeforeEach
    void before(@TempDir Path tmpPath) {
        indexFilePath = tmpPath.resolve(UUID.randomUUID().toString());
    }

    @Test
    void testFindNonExistentRegion() throws Exception {
        CompletableFuture<Void> cachedRegionFuture = new CompletableFuture<>();
        try (HsFileDataIndexSpilledRegionManager spilledRegionManager =
                createSpilledRegionManager(
                        (ignore1, ignore2) -> cachedRegionFuture.complete(null))) {
            long regionOffset = spilledRegionManager.findRegion(0, 0, true);
            assertThat(regionOffset).isEqualTo(-1);
            assertThat(cachedRegionFuture).isNotCompleted();
        }
    }

    @Test
    void testAppendOrOverwriteRegion() throws Exception {
        CompletableFuture<Void> cachedRegionFuture = new CompletableFuture<>();
        try (HsFileDataIndexSpilledRegionManager spilledRegionManager =
                createSpilledRegionManager(
                        (ignore1, ignore2) -> cachedRegionFuture.complete(null))) {
            InternalRegion region = createSingleUnreleasedRegion(0, 0L, 1);
            // append region to index file.
            spilledRegionManager.appendOrOverwriteRegion(0, region);
            assertThat(cachedRegionFuture).isNotCompleted();
            FileChannel indexFileChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
            InternalRegion readRegion =
                    InternalRegionWriteReadUtils.readRegionFromFile(
                            indexFileChannel,
                            allocateAndConfigureBuffer(InternalRegion.HEADER_SIZE),
                            0L);
            assertRegionEquals(readRegion, region);

            // new region must have the same size of old region.
            InternalRegion newRegion = createSingleUnreleasedRegion(0, 10L, 1);
            // overwrite old region.
            spilledRegionManager.appendOrOverwriteRegion(0, newRegion);
            // appendOrOverwriteRegion will not trigger cache load.
            assertThat(cachedRegionFuture).isNotCompleted();
            InternalRegion readNewRegion =
                    InternalRegionWriteReadUtils.readRegionFromFile(
                            indexFileChannel,
                            allocateAndConfigureBuffer(InternalRegion.HEADER_SIZE),
                            0L);
            assertRegionEquals(readNewRegion, newRegion);
        }
    }

    @Test
    void testWriteMoreThanOneSegment() throws Exception {
        List<InternalRegion> regions = createAllUnreleasedRegions(0, 0L, 2, 2);
        int segmentSize = regions.stream().mapToInt(InternalRegion::getSize).sum() + 1;
        try (HsFileDataIndexSpilledRegionManager spilledRegionManager =
                createSpilledRegionManager(segmentSize, (ignore1, ignore2) -> {})) {
            spilledRegionManager.appendOrOverwriteRegion(0, regions.get(0));
            spilledRegionManager.appendOrOverwriteRegion(0, regions.get(1));
            // segment has no enough space, will start new segment.
            InternalRegion regionInNewSegment = createSingleUnreleasedRegion(4, 4L, 2);
            spilledRegionManager.appendOrOverwriteRegion(0, regionInNewSegment);
            FileChannel indexFileChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
            InternalRegion readRegion =
                    InternalRegionWriteReadUtils.readRegionFromFile(
                            indexFileChannel,
                            allocateAndConfigureBuffer(InternalRegion.HEADER_SIZE),
                            // offset is segment size instead of two regions size to prove that new
                            // segment is started.
                            segmentSize);
            assertRegionEquals(readRegion, regionInNewSegment);
        }
    }

    @Test
    void testWriteBigRegion() throws Exception {
        int segmentSize = 4;
        try (HsFileDataIndexSpilledRegionManager spilledRegionManager =
                createSpilledRegionManager(segmentSize, (ignore1, ignore2) -> {})) {
            List<InternalRegion> regions = createAllUnreleasedRegions(0, 0L, 1, 2);
            InternalRegion region1 = regions.get(0);
            InternalRegion region2 = regions.get(1);
            assertThat(region1.getSize()).isGreaterThan(segmentSize);
            assertThat(region2.getSize()).isGreaterThan(segmentSize);

            spilledRegionManager.appendOrOverwriteRegion(0, region1);
            spilledRegionManager.appendOrOverwriteRegion(0, region2);
            FileChannel indexFileChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
            InternalRegion readRegion1 =
                    InternalRegionWriteReadUtils.readRegionFromFile(
                            indexFileChannel,
                            allocateAndConfigureBuffer(InternalRegion.HEADER_SIZE),
                            0L);
            assertRegionEquals(readRegion1, region1);

            InternalRegion readRegion2 =
                    InternalRegionWriteReadUtils.readRegionFromFile(
                            indexFileChannel,
                            allocateAndConfigureBuffer(InternalRegion.HEADER_SIZE),
                            readRegion1.getSize());
            assertRegionEquals(readRegion2, region2);
        }
    }

    @Test
    void testFindRegionFirstBufferIndexInMultipleSegments() throws Exception {
        final int numBuffersPerRegion = 2;
        final int subpartition = 0;
        List<InternalRegion> loadedRegions = new ArrayList<>();
        try (HsFileDataIndexSpilledRegionManager spilledRegionManager =
                createSpilledRegionManager(
                        // every segment can store two regions.
                        (InternalRegion.HEADER_SIZE + numBuffersPerRegion) * 2,
                        (ignore, region) -> loadedRegions.add(region))) {
            // segment1: region1(0-2), region2(9-11), min:0, max: 11
            spilledRegionManager.appendOrOverwriteRegion(
                    subpartition, createSingleUnreleasedRegion(0, 0L, numBuffersPerRegion));
            spilledRegionManager.appendOrOverwriteRegion(
                    subpartition, createSingleUnreleasedRegion(9, 9L, numBuffersPerRegion));

            // segment2: region1(2-4), region2(11-13) min: 2, max: 13
            InternalRegion targetRegion = createSingleUnreleasedRegion(2, 2L, 2);
            spilledRegionManager.appendOrOverwriteRegion(subpartition, targetRegion);
            spilledRegionManager.appendOrOverwriteRegion(
                    subpartition, createSingleUnreleasedRegion(11, 11L, 2));

            // segment3: region1(7-9) min: 7, max: 9
            spilledRegionManager.appendOrOverwriteRegion(
                    subpartition, createSingleUnreleasedRegion(7, 7L, 2));

            // find target region
            long regionOffset = spilledRegionManager.findRegion(subpartition, 3, true);
            assertThat(regionOffset).isNotEqualTo(-1L);
            assertThat(loadedRegions).hasSize(2);
            // target region must be put to the cache last.
            assertRegionEquals(loadedRegions.get(1), targetRegion);
        }
    }

    private HsFileDataIndexSpilledRegionManager createSpilledRegionManager(
            BiConsumer<Integer, InternalRegion> cacheRegionConsumer) {
        return createSpilledRegionManager(256, cacheRegionConsumer);
    }

    private HsFileDataIndexSpilledRegionManager createSpilledRegionManager(
            int segmentSize, BiConsumer<Integer, InternalRegion> cacheRegionConsumer) {
        int numSubpartitions = 2;
        return new HsFileDataIndexSpilledRegionManagerImpl.Factory(segmentSize, Long.MAX_VALUE)
                .create(numSubpartitions, indexFilePath, cacheRegionConsumer);
    }
}
