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

package org.apache.flink.runtime.io.network.partition.hybrid.index;

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
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createSingleTestRegion;
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createTestRegions;
import static org.apache.flink.runtime.io.network.partition.hybrid.index.TestingFileDataIndexRegion.readRegionFromFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FileDataIndexSpilledRegionManagerImpl}. */
class FileDataIndexSpilledRegionManagerImplTest {
    private Path indexFilePath;

    @BeforeEach
    void before(@TempDir Path tmpPath) {
        indexFilePath = tmpPath.resolve(UUID.randomUUID().toString());
    }

    @Test
    void testFindNonExistentRegion() throws Exception {
        CompletableFuture<Void> cachedRegionFuture = new CompletableFuture<>();
        try (FileDataIndexSpilledRegionManager<TestingFileDataIndexRegion> spilledRegionManager =
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
        try (FileDataIndexSpilledRegionManager<TestingFileDataIndexRegion> spilledRegionManager =
                createSpilledRegionManager(
                        (ignore1, ignore2) -> cachedRegionFuture.complete(null))) {
            TestingFileDataIndexRegion region = createSingleTestRegion(0, 0L, 1);
            // append region to index file.
            spilledRegionManager.appendOrOverwriteRegion(0, region);
            assertThat(cachedRegionFuture).isNotCompleted();
            FileChannel indexFileChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
            TestingFileDataIndexRegion readRegion = readRegionFromFile(indexFileChannel, 0L);
            assertRegionEquals(readRegion, region);

            // new region must have the same size of old region.
            TestingFileDataIndexRegion newRegion = createSingleTestRegion(0, 10L, 1);
            // overwrite old region.
            spilledRegionManager.appendOrOverwriteRegion(0, newRegion);
            // appendOrOverwriteRegion will not trigger cache load.
            assertThat(cachedRegionFuture).isNotCompleted();
            TestingFileDataIndexRegion readNewRegion = readRegionFromFile(indexFileChannel, 0L);
            assertRegionEquals(readNewRegion, newRegion);
        }
    }

    @Test
    void testWriteMoreThanOneRegionGroup() throws Exception {
        List<TestingFileDataIndexRegion> regions = createTestRegions(0, 0L, 2, 2);
        int regionGroupSize =
                regions.stream().mapToInt(TestingFileDataIndexRegion::getSize).sum() + 1;
        try (FileDataIndexSpilledRegionManager<TestingFileDataIndexRegion> spilledRegionManager =
                createSpilledRegionManager(regionGroupSize, (ignore1, ignore2) -> {})) {
            spilledRegionManager.appendOrOverwriteRegion(0, regions.get(0));
            spilledRegionManager.appendOrOverwriteRegion(0, regions.get(1));
            // region group has no enough space, will start new region group.
            TestingFileDataIndexRegion regionInNewRegionGroup = createSingleTestRegion(4, 4L, 2);
            spilledRegionManager.appendOrOverwriteRegion(0, regionInNewRegionGroup);
            FileChannel indexFileChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
            TestingFileDataIndexRegion readRegion =
                    readRegionFromFile(
                            indexFileChannel,
                            // offset is region group size instead of two regions size to prove that
                            // new region group is started.
                            regionGroupSize);
            assertRegionEquals(readRegion, regionInNewRegionGroup);
        }
    }

    @Test
    void testWriteBigRegion() throws Exception {
        int regionGroupSize = 4;
        try (FileDataIndexSpilledRegionManager<TestingFileDataIndexRegion> spilledRegionManager =
                createSpilledRegionManager(regionGroupSize, (ignore1, ignore2) -> {})) {
            List<TestingFileDataIndexRegion> regions = createTestRegions(0, 0L, 1, 2);
            TestingFileDataIndexRegion region1 = regions.get(0);
            TestingFileDataIndexRegion region2 = regions.get(1);
            assertThat(region1.getSize()).isGreaterThan(regionGroupSize);
            assertThat(region2.getSize()).isGreaterThan(regionGroupSize);

            spilledRegionManager.appendOrOverwriteRegion(0, region1);
            spilledRegionManager.appendOrOverwriteRegion(0, region2);
            FileChannel indexFileChannel = FileChannel.open(indexFilePath, StandardOpenOption.READ);
            TestingFileDataIndexRegion readRegion1 = readRegionFromFile(indexFileChannel, 0L);
            assertRegionEquals(readRegion1, region1);

            TestingFileDataIndexRegion readRegion2 =
                    readRegionFromFile(indexFileChannel, readRegion1.getSize());
            assertRegionEquals(readRegion2, region2);
        }
    }

    @Test
    void testFindRegionFirstBufferIndexInMultipleRegionGroups() throws Exception {
        final int numBuffersPerRegion = 2;
        final int subpartition = 0;
        List<TestingFileDataIndexRegion> loadedRegions = new ArrayList<>();
        try (FileDataIndexSpilledRegionManager<TestingFileDataIndexRegion> spilledRegionManager =
                createSpilledRegionManager(
                        // every region group can store two regions.
                        TestingFileDataIndexRegion.REGION_SIZE * 2,
                        (ignore, region) -> loadedRegions.add(region))) {
            // region group1: region1(0-1), region2(9-10), min:0, max: 10
            spilledRegionManager.appendOrOverwriteRegion(
                    subpartition, createSingleTestRegion(0, 0L, numBuffersPerRegion));
            spilledRegionManager.appendOrOverwriteRegion(
                    subpartition, createSingleTestRegion(9, 9L, numBuffersPerRegion));

            // region group2: region1(2-3), region2(11-12) min: 2, max: 12
            TestingFileDataIndexRegion targetRegion =
                    createSingleTestRegion(2, 2L, numBuffersPerRegion);
            spilledRegionManager.appendOrOverwriteRegion(subpartition, targetRegion);
            spilledRegionManager.appendOrOverwriteRegion(
                    subpartition, createSingleTestRegion(11, 11L, numBuffersPerRegion));

            // region group3: region1(7-8) min: 7, max: 8
            spilledRegionManager.appendOrOverwriteRegion(
                    subpartition, createSingleTestRegion(7, 7L, numBuffersPerRegion));

            // find target region
            long regionOffset = spilledRegionManager.findRegion(subpartition, 3, true);
            assertThat(regionOffset).isNotEqualTo(-1L);
            assertThat(loadedRegions).hasSize(2);
            // target region must be put to the cache last.
            assertRegionEquals(loadedRegions.get(1), targetRegion);
        }
    }

    private FileDataIndexSpilledRegionManager<TestingFileDataIndexRegion>
            createSpilledRegionManager(
                    BiConsumer<Integer, TestingFileDataIndexRegion> cacheRegionConsumer) {
        return createSpilledRegionManager(256, cacheRegionConsumer);
    }

    private FileDataIndexSpilledRegionManager<TestingFileDataIndexRegion>
            createSpilledRegionManager(
                    int regionGroupSize,
                    BiConsumer<Integer, TestingFileDataIndexRegion> cacheRegionConsumer) {
        int numSubpartitions = 2;
        return new FileDataIndexSpilledRegionManagerImpl.Factory<>(
                        regionGroupSize,
                        Long.MAX_VALUE,
                        TestingFileDataIndexRegion.REGION_SIZE,
                        new TestingFileDataIndexRegionHelper.Builder()
                                .setReadRegionFromFileFunction(
                                        TestingFileDataIndexRegion::readRegionFromFile)
                                .setWriteRegionToFileConsumer(
                                        TestingFileDataIndexRegion::writeRegionToFile)
                                .build())
                .create(numSubpartitions, indexFilePath, cacheRegionConsumer);
    }
}
