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

import org.apache.flink.util.ExceptionUtils;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Default implementation of {@link HsFileDataIndex}. */
@ThreadSafe
public class HsFileDataIndexImpl implements HsFileDataIndex {

    @GuardedBy("lock")
    private final HsFileDataIndexCache indexCache;

    /**
     * {@link HsFileDataIndexCache} is not thread-safe, any access to it needs to hold this lock.
     */
    private final Object lock = new Object();

    public HsFileDataIndexImpl(
            int numSubpartitions,
            Path indexFilePath,
            int spilledIndexSegmentSize,
            long numRetainedInMemoryRegionsMax) {
        this.indexCache =
                new HsFileDataIndexCache(
                        numSubpartitions,
                        indexFilePath,
                        numRetainedInMemoryRegionsMax,
                        new HsFileDataIndexSpilledRegionManagerImpl.Factory(
                                spilledIndexSegmentSize, numRetainedInMemoryRegionsMax));
    }

    @Override
    public void close() {
        synchronized (lock) {
            try {
                indexCache.close();
            } catch (IOException e) {
                ExceptionUtils.rethrow(e);
            }
        }
    }

    @Override
    public Optional<ReadableRegion> getReadableRegion(
            int subpartitionId, int bufferIndex, int consumingOffset) {
        synchronized (lock) {
            return getInternalRegion(subpartitionId, bufferIndex)
                    .map(
                            internalRegion ->
                                    internalRegion.toReadableRegion(bufferIndex, consumingOffset))
                    .filter(internalRegion -> internalRegion.numReadable > 0);
        }
    }

    @Override
    public void addBuffers(List<SpilledBuffer> spilledBuffers) {
        final Map<Integer, List<InternalRegion>> subpartitionInternalRegions =
                convertToInternalRegions(spilledBuffers);
        synchronized (lock) {
            subpartitionInternalRegions.forEach(indexCache::put);
        }
    }

    @Override
    public void markBufferReleased(int subpartitionId, int bufferIndex) {
        synchronized (lock) {
            getInternalRegion(subpartitionId, bufferIndex)
                    .ifPresent(internalRegion -> internalRegion.markBufferReleased(bufferIndex));
        }
    }

    @GuardedBy("lock")
    private Optional<InternalRegion> getInternalRegion(int subpartitionId, int bufferIndex) {
        return indexCache.get(subpartitionId, bufferIndex);
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
    static class InternalRegion {
        /**
         * {@link InternalRegion} is consists of header and payload. (firstBufferIndex,
         * firstBufferOffset, numBuffer) are immutable header part that have fixed size. The array
         * of released is variable payload. This field represents the size of header.
         */
        public static final int HEADER_SIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;

        private final int firstBufferIndex;
        private final long firstBufferOffset;
        private final int numBuffers;
        private final boolean[] released;

        private InternalRegion(int firstBufferIndex, long firstBufferOffset, int numBuffers) {
            this.firstBufferIndex = firstBufferIndex;
            this.firstBufferOffset = firstBufferOffset;
            this.numBuffers = numBuffers;
            this.released = new boolean[numBuffers];
            Arrays.fill(released, false);
        }

        InternalRegion(
                int firstBufferIndex, long firstBufferOffset, int numBuffers, boolean[] released) {
            this.firstBufferIndex = firstBufferIndex;
            this.firstBufferOffset = firstBufferOffset;
            this.numBuffers = numBuffers;
            this.released = released;
        }

        boolean containBuffer(int bufferIndex) {
            return bufferIndex >= firstBufferIndex && bufferIndex < firstBufferIndex + numBuffers;
        }

        private HsFileDataIndex.ReadableRegion toReadableRegion(
                int bufferIndex, int consumingOffset) {
            int nSkip = bufferIndex - firstBufferIndex;
            int nReadable = 0;
            while (nSkip + nReadable < numBuffers) {
                if (!released[nSkip + nReadable] || (bufferIndex + nReadable) <= consumingOffset) {
                    break;
                }
                ++nReadable;
            }
            return new ReadableRegion(nSkip, nReadable, firstBufferOffset);
        }

        private void markBufferReleased(int bufferIndex) {
            released[bufferIndex - firstBufferIndex] = true;
        }

        /** Get the total size in bytes of this region, including header and payload. */
        int getSize() {
            return HEADER_SIZE + numBuffers;
        }

        int getFirstBufferIndex() {
            return firstBufferIndex;
        }

        long getFirstBufferOffset() {
            return firstBufferOffset;
        }

        int getNumBuffers() {
            return numBuffers;
        }

        boolean[] getReleased() {
            return released;
        }
    }
}
