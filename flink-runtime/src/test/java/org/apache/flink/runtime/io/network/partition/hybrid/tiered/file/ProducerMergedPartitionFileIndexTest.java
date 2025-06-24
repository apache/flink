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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ProducerMergedPartitionFileIndex}. */
class ProducerMergedPartitionFileIndexTest {

    private Path indexFilePath;

    @BeforeEach
    void before(@TempDir Path tempDir) {
        this.indexFilePath = tempDir.resolve(".index");
    }

    @Test
    void testAddBufferAndGetRegion() {
        int numSubpartitions = 5;
        int numBuffersPerSubpartition = 10;

        ProducerMergedPartitionFileIndex partitionFileIndex =
                new ProducerMergedPartitionFileIndex(
                        numSubpartitions, indexFilePath, 256, Long.MAX_VALUE);

        List<ProducerMergedPartitionFileIndex.FlushedBuffer> flushedBuffers = new ArrayList<>();
        Tuple2<Integer, Integer> numExpectedRegionsAndMaxBufferIndex =
                generateFlushedBuffers(numSubpartitions, numBuffersPerSubpartition, flushedBuffers);
        partitionFileIndex.addBuffers(flushedBuffers);
        int numExpectedRegions = numExpectedRegionsAndMaxBufferIndex.f0;
        int maxBufferIndex = numExpectedRegionsAndMaxBufferIndex.f1;

        int numGetRegions =
                numGetRegionsFromIndex(numSubpartitions, partitionFileIndex, maxBufferIndex);
        partitionFileIndex.release();

        assertThat(numExpectedRegions).isEqualTo(numGetRegions);
    }

    private Tuple2<Integer, Integer> generateFlushedBuffers(
            int numSubpartitions,
            int numBuffersPerSubpartition,
            List<ProducerMergedPartitionFileIndex.FlushedBuffer> flushedBuffers) {
        int numExpectedRegions = 0;
        int maxBufferIndex = 0;
        Random random = new Random();
        for (int i = 0; i < numSubpartitions; i++) {
            int bufferIndex = 0;
            for (int j = 0; j < numBuffersPerSubpartition; j++) {
                boolean isNextRegionContinuous =
                        (j == 0 || random.nextBoolean()) && j != numBuffersPerSubpartition - 1;
                flushedBuffers.add(
                        new ProducerMergedPartitionFileIndex.FlushedBuffer(i, bufferIndex, 0, 1));
                bufferIndex++;

                if (!isNextRegionContinuous) {
                    bufferIndex++;
                    numExpectedRegions++;
                }
                maxBufferIndex = Math.max(bufferIndex, maxBufferIndex);
            }
        }
        return new Tuple2<>(numExpectedRegions, maxBufferIndex);
    }

    private static int numGetRegionsFromIndex(
            int numSubpartitions,
            ProducerMergedPartitionFileIndex partitionFileIndex,
            int maxBufferIndex) {
        List<Set<Integer>> subpartitionFirstBufferIndexes = new ArrayList<>();
        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionFirstBufferIndexes.add(new HashSet<>());
            for (int j = 0; j <= maxBufferIndex; j++) {
                Optional<ProducerMergedPartitionFileIndex.FixedSizeRegion> region =
                        partitionFileIndex.getRegion(new TieredStorageSubpartitionId(i), j);
                if (region.isPresent()) {
                    subpartitionFirstBufferIndexes.get(i).add(region.get().getFirstBufferIndex());
                }
            }
        }

        // The buffer indexes in different regions are different, so using the first buffer index to
        // uniquely represent one region.
        return subpartitionFirstBufferIndexes.stream().mapToInt(Set::size).sum();
    }
}
