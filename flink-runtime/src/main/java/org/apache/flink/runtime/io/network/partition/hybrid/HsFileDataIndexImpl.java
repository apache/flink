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

import org.apache.flink.runtime.io.network.partition.hybrid.index.FileDataIndexCache;
import org.apache.flink.runtime.io.network.partition.hybrid.index.FileDataIndexRegionHelper;
import org.apache.flink.runtime.io.network.partition.hybrid.index.FileDataIndexSpilledRegionManagerImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.index.FileRegionWriteReadUtils;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.partition.hybrid.index.FileRegionWriteReadUtils.allocateAndConfigureBuffer;
import static org.apache.flink.util.Preconditions.checkArgument;

/** Default implementation of {@link HsFileDataIndex}. */
@ThreadSafe
public class HsFileDataIndexImpl implements HsFileDataIndex {

    @GuardedBy("lock")
    private final FileDataIndexCache<InternalRegion> indexCache;

    /** {@link FileDataIndexCache} is not thread-safe, any access to it needs to hold this lock. */
    private final Object lock = new Object();

    public HsFileDataIndexImpl(
            int numSubpartitions,
            Path indexFilePath,
            int regionGroupSizeInBytes,
            long numRetainedInMemoryRegionsMax) {
        this.indexCache =
                new FileDataIndexCache<>(
                        numSubpartitions,
                        indexFilePath,
                        numRetainedInMemoryRegionsMax,
                        new FileDataIndexSpilledRegionManagerImpl.Factory<>(
                                regionGroupSizeInBytes,
                                numRetainedInMemoryRegionsMax,
                                InternalRegion.HEADER_SIZE,
                                HsFileDataIndexRegionHelper.INSTANCE));
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
                .computeIfAbsent(firstBufferInRegion.subpartitionId, k -> new ArrayList<>())
                .add(
                        new InternalRegion(
                                firstBufferInRegion.bufferIndex,
                                firstBufferInRegion.fileOffset,
                                lastBufferInRegion.bufferIndex
                                        - firstBufferInRegion.bufferIndex
                                        + 1));
    }

    /**
     * A {@link InternalRegion} is an implementation of {@link FileDataIndexRegionHelper.Region}.
     * Note that this class introduced a new field to indicate whether each buffer in the region is
     * released.
     */
    public static class InternalRegion implements FileDataIndexRegionHelper.Region {
        /**
         * {@link InternalRegion} is consists of header and payload. (firstBufferIndex,
         * firstBufferOffset, numBuffer) are immutable header part that have fixed size. The array
         * of released is variable payload. This field represents the size of header.
         */
        public static final int HEADER_SIZE = Integer.BYTES + Long.BYTES + Integer.BYTES;

        private final int firstBufferIndex;
        private final long regionFileOffset;
        private final int numBuffers;
        private final boolean[] released;

        private InternalRegion(int firstBufferIndex, long regionFileOffset, int numBuffers) {
            this.firstBufferIndex = firstBufferIndex;
            this.regionFileOffset = regionFileOffset;
            this.numBuffers = numBuffers;
            this.released = new boolean[numBuffers];
            Arrays.fill(released, false);
        }

        public InternalRegion(
                int firstBufferIndex, long regionFileOffset, int numBuffers, boolean[] released) {
            this.firstBufferIndex = firstBufferIndex;
            this.regionFileOffset = regionFileOffset;
            this.numBuffers = numBuffers;
            this.released = released;
        }

        @Override
        public boolean containBuffer(int bufferIndex) {
            return bufferIndex >= firstBufferIndex && bufferIndex < firstBufferIndex + numBuffers;
        }

        @Override
        public int getSize() {
            return HEADER_SIZE + numBuffers;
        }

        @Override
        public int getFirstBufferIndex() {
            return firstBufferIndex;
        }

        @Override
        public long getRegionStartOffset() {
            return regionFileOffset;
        }

        @Override
        public long getRegionEndOffset() {
            throw new UnsupportedOperationException("This method is not supported.");
        }

        @Override
        public int getNumBuffers() {
            return numBuffers;
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
            return new ReadableRegion(nSkip, nReadable, regionFileOffset);
        }

        private void markBufferReleased(int bufferIndex) {
            released[bufferIndex - firstBufferIndex] = true;
        }

        public boolean[] getReleased() {
            return released;
        }
    }

    /**
     * The implementation of {@link FileDataIndexRegionHelper} to writing a region to the file or
     * reading a region from the file.
     *
     * <p>Note that this type of region's length may be variable because it contains an array to
     * indicate each buffer's release state.
     */
    public static class HsFileDataIndexRegionHelper
            implements FileDataIndexRegionHelper<InternalRegion> {

        /** Reusable buffer used to read and write the immutable part of region. */
        private final ByteBuffer regionHeaderBuffer =
                allocateAndConfigureBuffer(HsFileDataIndexImpl.InternalRegion.HEADER_SIZE);

        public static final HsFileDataIndexRegionHelper INSTANCE =
                new HsFileDataIndexRegionHelper();

        private HsFileDataIndexRegionHelper() {}

        @Override
        public void writeRegionToFile(FileChannel channel, InternalRegion region)
                throws IOException {
            FileRegionWriteReadUtils.writeHsInternalRegionToFile(
                    channel, regionHeaderBuffer, region);
        }

        @Override
        public InternalRegion readRegionFromFile(FileChannel channel, long fileOffset)
                throws IOException {
            return FileRegionWriteReadUtils.readHsInternalRegionFromFile(
                    channel, regionHeaderBuffer, fileOffset);
        }
    }
}
