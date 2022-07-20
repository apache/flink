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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Default implementation of {@link HsFileDataIndex}. */
@ThreadSafe
public class HsFileDataIndexImpl implements HsFileDataIndex {

    @GuardedBy("lock")
    private final List<TreeMap<Integer, InternalRegion>>
            subpartitionFirstBufferIndexInternalRegions;

    private final Object lock = new Object();

    public HsFileDataIndexImpl(int numSubpartitions) {
        this.subpartitionFirstBufferIndexInternalRegions = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionFirstBufferIndexInternalRegions.add(new TreeMap<>());
        }
    }

    @Override
    public Optional<ReadableRegion> getReadableRegion(int subpartitionId, int bufferIndex) {
        synchronized (lock) {
            return getInternalRegion(subpartitionId, bufferIndex)
                    .map(internalRegion -> internalRegion.toReadableRegion(bufferIndex))
                    .filter(internalRegion -> internalRegion.numReadable > 0);
        }
    }

    @Override
    public void addBuffers(List<SpilledBuffer> spilledBuffers) {
        final Map<Integer, List<InternalRegion>> subpartitionInternalRegions =
                convertToInternalRegions(spilledBuffers);
        synchronized (lock) {
            subpartitionInternalRegions.forEach(
                    (subpartition, internalRegions) -> {
                        TreeMap<Integer, InternalRegion> treeMap =
                                subpartitionFirstBufferIndexInternalRegions.get(subpartition);
                        for (InternalRegion internalRegion : internalRegions) {
                            treeMap.put(internalRegion.firstBufferIndex, internalRegion);
                        }
                    });
        }
    }

    @Override
    public void markBufferReadable(int subpartitionId, int bufferIndex) {
        synchronized (lock) {
            getInternalRegion(subpartitionId, bufferIndex)
                    .ifPresent(internalRegion -> internalRegion.markBufferReadable(bufferIndex));
        }
    }

    @GuardedBy("lock")
    private Optional<InternalRegion> getInternalRegion(int subpartitionId, int bufferIndex) {
        return Optional.ofNullable(
                        subpartitionFirstBufferIndexInternalRegions
                                .get(subpartitionId)
                                .floorEntry(bufferIndex))
                .map(Map.Entry::getValue)
                .filter(internalRegion -> internalRegion.containBuffer(bufferIndex));
    }

    private static Map<Integer, List<InternalRegion>> convertToInternalRegions(
            List<SpilledBuffer> spilledBuffers) {

        if (spilledBuffers.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<Integer, List<InternalRegion>> internalRegionsBySubpartition = new HashMap<>();
        final Iterator<SpilledBuffer> iterator = spilledBuffers.iterator();
        // There's at least one buffer
        SpilledBuffer firstBufferOfCurrentRegion = iterator.next();
        SpilledBuffer lastBufferOfCurrentRegion = firstBufferOfCurrentRegion;

        while (iterator.hasNext()) {
            SpilledBuffer currentBuffer = iterator.next();

            if (currentBuffer.subpartitionId != firstBufferOfCurrentRegion.subpartitionId
                    || currentBuffer.bufferIndex != lastBufferOfCurrentRegion.bufferIndex + 1) {
                // the current buffer belongs to a new region, close the previous region
                addInternalRegionToMap(
                        firstBufferOfCurrentRegion,
                        lastBufferOfCurrentRegion,
                        internalRegionsBySubpartition);
                firstBufferOfCurrentRegion = currentBuffer;
            }

            lastBufferOfCurrentRegion = currentBuffer;
        }

        // close the last region
        addInternalRegionToMap(
                firstBufferOfCurrentRegion,
                lastBufferOfCurrentRegion,
                internalRegionsBySubpartition);

        return internalRegionsBySubpartition;
    }

    private static void addInternalRegionToMap(
            SpilledBuffer firstBufferInRegion,
            SpilledBuffer lastBufferInRegion,
            Map<Integer, List<InternalRegion>> internalRegionsBySubpartition) {
        checkArgument(firstBufferInRegion.subpartitionId == lastBufferInRegion.subpartitionId);
        checkArgument(firstBufferInRegion.bufferIndex <= lastBufferInRegion.bufferIndex);
        internalRegionsBySubpartition
                .computeIfAbsent(firstBufferInRegion.subpartitionId, ArrayList::new)
                .add(
                        new InternalRegion(
                                firstBufferInRegion.bufferIndex,
                                firstBufferInRegion.fileOffset,
                                lastBufferInRegion.bufferIndex
                                        - firstBufferInRegion.bufferIndex
                                        + 1));
    }

    /**
     * A {@link InternalRegion} represents a series of physically continuous buffers in the file,
     * which are from the same subpartition, and has sequential buffer index.
     *
     * <p>The following example illustrates some physically continuous buffers in a file and regions
     * upon them, where `x-y` denotes buffer from subpartition x with buffer index y, and `()`
     * denotes a region.
     *
     * <p>(1-1, 1-2), (2-1), (2-2, 2-3), (1-5, 1-6), (1-4)
     *
     * <p>Note: The file may not contain all the buffers. E.g., 1-3 is missing in the above example.
     *
     * <p>Note: Buffers in file may have different orders than their buffer index. E.g., 1-4 comes
     * after 1-6 in the above example.
     *
     * <p>Note: This index may not always maintain the longest possible regions. E.g., 2-1, 2-2, 2-3
     * are in two separate regions.
     */
    private static class InternalRegion {
        private final int firstBufferIndex;
        private final long firstBufferOffset;
        private final int numBuffers;
        private final boolean[] readable;

        private InternalRegion(int firstBufferIndex, long firstBufferOffset, int numBuffers) {
            this.firstBufferIndex = firstBufferIndex;
            this.firstBufferOffset = firstBufferOffset;
            this.numBuffers = numBuffers;
            this.readable = new boolean[numBuffers];
            Arrays.fill(readable, false);
        }

        private boolean containBuffer(int bufferIndex) {
            return bufferIndex >= firstBufferIndex && bufferIndex < firstBufferIndex + numBuffers;
        }

        private HsFileDataIndex.ReadableRegion toReadableRegion(int bufferIndex) {
            int nSkip = bufferIndex - firstBufferIndex;
            int nReadable = 0;
            while (nSkip + nReadable < numBuffers) {
                if (!readable[nSkip + nReadable]) {
                    break;
                }
                ++nReadable;
            }
            return new ReadableRegion(nSkip, nReadable, firstBufferOffset);
        }

        private void markBufferReadable(int bufferIndex) {
            readable[bufferIndex - firstBufferIndex] = true;
        }
    }
}
