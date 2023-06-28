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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The {@link ProducerMergedPartitionFileIndex} is used by {@link ProducerMergedPartitionFileWriter}
 * and {@link ProducerMergedPartitionFileReader}, to maintain the offset of each buffer in the
 * physical file.
 *
 * <p>For efficiency, buffers from the same subpartition that are both logically (i.e. index in the
 * subpartition) and physically (i.e. offset in the file) consecutive are combined into a {@link
 * Region}.
 *
 * <pre>For example, the following buffers (indicated by subpartitionId-bufferIndex):
 *   1-1, 1-2, 1-3, 2-1, 2-2, 2-5, 1-4, 1-5, 2-6
 * will be combined into 5 regions (separated by '|'):
 *   1-1, 1-2, 1-3 | 2-1, 2-2 | 2-5 | 1-4, 1-5 | 2-6
 * </pre>
 */
public class ProducerMergedPartitionFileIndex {

    /**
     * The regions belonging to each subpartitions.
     *
     * <p>Note that the field can be accessed by the writing and reading IO thread, so the lock is
     * to ensure the thread safety.
     */
    @GuardedBy("lock")
    private final List<TreeMap<Integer, Region>> subpartitionRegions;

    @GuardedBy("lock")
    private boolean isReleased;

    private final Object lock = new Object();

    public ProducerMergedPartitionFileIndex(int numSubpartitions) {
        this.subpartitionRegions = new ArrayList<>();
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionRegions.add(new TreeMap<>());
        }
    }

    /**
     * Add buffers to the index.
     *
     * @param buffers to be added. Note, the provided buffers are required to be physically
     *     consecutive and in the same order as in the file.
     */
    void addBuffers(List<FlushedBuffer> buffers) {
        if (buffers.isEmpty()) {
            return;
        }

        Map<Integer, List<Region>> convertedRegions = convertToRegions(buffers);
        synchronized (lock) {
            convertedRegions.forEach(
                    (subpartition, regions) -> {
                        Map<Integer, Region> regionMap = subpartitionRegions.get(subpartition);
                        for (Region region : regions) {
                            regionMap.put(region.getFirstBufferIndex(), region);
                        }
                    });
        }
    }

    /**
     * Get the subpartition's {@link Region} containing the specific buffer index.
     *
     * @param subpartitionId the subpartition id
     * @param bufferIndex the buffer index
     * @return the region containing the buffer index, or return emtpy if the region is not found.
     */
    Optional<Region> getRegion(TieredStorageSubpartitionId subpartitionId, int bufferIndex) {
        synchronized (lock) {
            if (isReleased) {
                return Optional.empty();
            }
            Map.Entry<Integer, Region> regionEntry =
                    subpartitionRegions
                            .get(subpartitionId.getSubpartitionId())
                            .floorEntry(bufferIndex);
            if (regionEntry == null) {
                return Optional.empty();
            }
            Region region = regionEntry.getValue();
            return bufferIndex < region.getFirstBufferIndex() + region.numBuffers
                    ? Optional.of(region)
                    : Optional.empty();
        }
    }

    void release() {
        synchronized (lock) {
            subpartitionRegions.clear();
            isReleased = true;
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private static Map<Integer, List<Region>> convertToRegions(List<FlushedBuffer> buffers) {
        Map<Integer, List<Region>> subpartitionRegionMap = new HashMap<>();
        Iterator<FlushedBuffer> iterator = buffers.iterator();
        FlushedBuffer firstBufferInRegion = iterator.next();
        FlushedBuffer lastBufferInRegion = firstBufferInRegion;

        while (iterator.hasNext()) {
            FlushedBuffer currentBuffer = iterator.next();
            if (currentBuffer.getSubpartitionId() != firstBufferInRegion.getSubpartitionId()
                    || currentBuffer.getBufferIndex() != lastBufferInRegion.getBufferIndex() + 1) {
                // The current buffer belongs to a new region, add the current region to the map
                addRegionToMap(firstBufferInRegion, lastBufferInRegion, subpartitionRegionMap);
                firstBufferInRegion = currentBuffer;
            }
            lastBufferInRegion = currentBuffer;
        }

        // Add the last region to the map
        addRegionToMap(firstBufferInRegion, lastBufferInRegion, subpartitionRegionMap);
        return subpartitionRegionMap;
    }

    private static void addRegionToMap(
            FlushedBuffer firstBufferInRegion,
            FlushedBuffer lastBufferInRegion,
            Map<Integer, List<Region>> subpartitionRegionMap) {
        checkArgument(
                firstBufferInRegion.getSubpartitionId() == lastBufferInRegion.getSubpartitionId());
        checkArgument(firstBufferInRegion.getBufferIndex() <= lastBufferInRegion.getBufferIndex());

        subpartitionRegionMap
                .computeIfAbsent(firstBufferInRegion.getSubpartitionId(), ArrayList::new)
                .add(
                        new Region(
                                firstBufferInRegion.getBufferIndex(),
                                firstBufferInRegion.getFileOffset(),
                                lastBufferInRegion.getBufferIndex()
                                        - firstBufferInRegion.getBufferIndex()
                                        + 1));
    }

    // ------------------------------------------------------------------------
    //  Internal Classes
    // ------------------------------------------------------------------------

    /** Represents a buffer to be flushed. */
    static class FlushedBuffer {
        /** The subpartition id that the buffer belongs to. */
        private final int subpartitionId;

        /** The buffer index within the subpartition. */
        private final int bufferIndex;

        /** The file offset that the buffer begin with. */
        private final long fileOffset;

        FlushedBuffer(int subpartitionId, int bufferIndex, long fileOffset) {
            this.subpartitionId = subpartitionId;
            this.bufferIndex = bufferIndex;
            this.fileOffset = fileOffset;
        }

        int getSubpartitionId() {
            return subpartitionId;
        }

        int getBufferIndex() {
            return bufferIndex;
        }

        long getFileOffset() {
            return fileOffset;
        }
    }

    /**
     * Represents a series of buffers that are:
     *
     * <ul>
     *   <li>From the same subpartition
     *   <li>Logically (i.e. buffer index) consecutive
     *   <li>Physically (i.e. offset in the file) consecutive
     * </ul>
     */
    static class Region {

        /** The buffer index of first buffer. */
        private final int firstBufferIndex;

        /** The file offset of the region. */
        private final long regionFileOffset;

        /** The number of buffers that the region contains. */
        private final int numBuffers;

        Region(int firstBufferIndex, long regionFileOffset, int numBuffers) {
            this.firstBufferIndex = firstBufferIndex;
            this.regionFileOffset = regionFileOffset;
            this.numBuffers = numBuffers;
        }

        long getRegionFileOffset() {
            return regionFileOffset;
        }

        int getNumBuffers() {
            return numBuffers;
        }

        int getFirstBufferIndex() {
            return firstBufferIndex;
        }
    }
}
