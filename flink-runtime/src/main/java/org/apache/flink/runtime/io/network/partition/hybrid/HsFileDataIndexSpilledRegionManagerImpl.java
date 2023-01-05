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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndexImpl.InternalRegion;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static org.apache.flink.runtime.io.network.partition.hybrid.InternalRegionWriteReadUtils.allocateAndConfigureBuffer;

/**
 * Default implementation of {@link HsFileDataIndexSpilledRegionManager}. This manager will handle
 * and spill regions in the following way:
 *
 * <ul>
 *   <li>All regions will be written to the same file, namely index file.
 *   <li>Multiple regions belonging to the same subpartition form a segment.
 *   <li>The regions in the same segment have no special relationship, but are only related to the
 *       order in which they are spilled.
 *   <li>Each segment is independent. Even if the previous segment is not full, the next segment can
 *       still be allocated.
 *   <li>If a region has been written to the index file already, spill it again will overwrite the
 *       previous region.
 *   <li>The very large region will monopolize a single segment.
 * </ul>
 */
public class HsFileDataIndexSpilledRegionManagerImpl
        implements HsFileDataIndexSpilledRegionManager {

    /** Reusable buffer used to read and write the immutable part of region. */
    private final ByteBuffer immutablePartBuffer =
            allocateAndConfigureBuffer(InternalRegion.FIXED_SIZE);

    /**
     * List of subpartition's segment meta. Each element is a treeMap contains all {@link
     * SegmentMeta}'s of specific subpartition corresponding to the subscript. The value of this
     * treeMap is a {@link SegmentMeta}, and the key is minBufferIndex of this segment. Only
     * finished(i.e. no longer appended) segment will be put to here.
     */
    private final List<TreeMap<Integer, SegmentMeta>> subpartitionFinishedSegmentMetas;

    private FileChannel channel;

    /** The Offset of next segment, new segment will start from this offset. */
    private long nextSegmentOffset = 0L;

    private final long[] subpartitionCurrentOffset;

    /** Free space of every subpartition's current segment. */
    private final int[] subpartitionFreeSpace;

    /** Metadata of every subpartition's current segment. */
    private final SegmentMeta[] currentSegmentMeta;

    /**
     * Default size of segment. If the size of a region is larger than this value, it will be
     * allocated and occupy a single segment.
     */
    private final int segmentSize;

    /**
     * This consumer is used to load region to cache. The first parameter is subpartition id, and
     * second parameter is the region to load.
     */
    private final BiConsumer<Integer, InternalRegion> cacheRegionConsumer;

    public HsFileDataIndexSpilledRegionManagerImpl(
            int numSubpartitions,
            Path indexFilePath,
            int segmentSize,
            BiConsumer<Integer, InternalRegion> cacheRegionConsumer) {
        try {
            this.channel =
                    FileChannel.open(
                            indexFilePath,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
        this.subpartitionFinishedSegmentMetas = new ArrayList<>(numSubpartitions);
        this.subpartitionCurrentOffset = new long[numSubpartitions];
        this.subpartitionFreeSpace = new int[numSubpartitions];
        this.currentSegmentMeta = new SegmentMeta[numSubpartitions];
        for (int i = 0; i < numSubpartitions; i++) {
            subpartitionFinishedSegmentMetas.add(new TreeMap<>());
        }
        this.cacheRegionConsumer = cacheRegionConsumer;
        this.segmentSize = segmentSize;
    }

    @Override
    public long findRegion(int subpartition, int bufferIndex, boolean loadToCache) {
        // first of all, find the region from current writing segment.
        SegmentMeta segmentMeta = currentSegmentMeta[subpartition];
        if (segmentMeta != null) {
            long regionOffset =
                    findRegionInSegment(subpartition, bufferIndex, segmentMeta, loadToCache);
            if (regionOffset != -1) {
                return regionOffset;
            }
        }

        // next, find the region from finished segments.
        TreeMap<Integer, SegmentMeta> subpartitionSegmentMetaTreeMap =
                subpartitionFinishedSegmentMetas.get(subpartition);
        // all segments with a minBufferIndex less than or equal to this target buffer index may
        // contain the target region.
        for (SegmentMeta meta :
                subpartitionSegmentMetaTreeMap.headMap(bufferIndex, true).values()) {
            long regionOffset = findRegionInSegment(subpartition, bufferIndex, meta, loadToCache);
            if (regionOffset != -1) {
                return regionOffset;
            }
        }
        return -1;
    }

    private long findRegionInSegment(
            int subpartition, int bufferIndex, SegmentMeta meta, boolean loadToCache) {
        if (bufferIndex < meta.getMaxBufferIndex()) {
            try {
                // read all regions belong to this segment.
                List<Tuple2<InternalRegion, Long>> regionAndOffsets =
                        readSegment(meta.getOffset(), meta.getNumRegions());
                for (Tuple2<InternalRegion, Long> regionAndOffset : regionAndOffsets) {
                    InternalRegion region = regionAndOffset.f0;
                    // whether the region contains this buffer.
                    if (region.containBuffer(bufferIndex)) {
                        // target region is founded.
                        if (loadToCache) {
                            // load this region to cache if needed.
                            cacheRegionConsumer.accept(subpartition, region);
                        }
                        // return the offset of target region.
                        return regionAndOffset.f1;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        // -1 indicates that target region is not founded from this segment.
        return -1;
    }

    @Override
    public void appendOrOverwriteRegion(int subpartition, InternalRegion newRegion)
            throws IOException {
        long oldRegionOffset = findRegion(subpartition, newRegion.getFirstBufferIndex(), false);
        if (oldRegionOffset != -1) {
            // if region is already exists in file, overwrite it.
            writeRegionToOffset(oldRegionOffset, newRegion);
        } else {
            // otherwise, append region to segment.
            appendRegion(subpartition, newRegion);
        }
    }

    @Override
    public void close() throws IOException {
        if (channel != null) {
            channel.close();
        }
    }

    private void appendRegion(int subpartition, InternalRegion region) throws IOException {
        int regionSize = region.getSize();
        // check whether we have enough space to append this region.
        if (subpartitionFreeSpace[subpartition] < regionSize) {
            // No enough free space, start a new segment. Note that if region is larger than
            // segment's size, this will start a new segment only contains the big region.
            startNewSegment(subpartition, Math.max(regionSize, segmentSize));
        }
        // spill this region to current offset of file index.
        writeRegionToOffset(subpartitionCurrentOffset[subpartition], region);
        // a new region was appended to segment, update it.
        updateSegment(subpartition, region);
    }

    private void writeRegionToOffset(long offset, InternalRegion region) throws IOException {
        channel.position(offset);
        InternalRegionWriteReadUtils.writeRegionToFile(channel, immutablePartBuffer, region);
    }

    private void startNewSegment(int subpartition, int newSegmentSize) {
        SegmentMeta oldSegmentMeta = currentSegmentMeta[subpartition];
        currentSegmentMeta[subpartition] = new SegmentMeta(nextSegmentOffset);
        subpartitionCurrentOffset[subpartition] = nextSegmentOffset;
        nextSegmentOffset += newSegmentSize;
        subpartitionFreeSpace[subpartition] = newSegmentSize;
        if (oldSegmentMeta != null) {
            // put the finished segment to subpartitionFinishedSegmentMetas.
            subpartitionFinishedSegmentMetas
                    .get(subpartition)
                    .put(oldSegmentMeta.minBufferIndex, oldSegmentMeta);
        }
    }

    private void updateSegment(int subpartition, InternalRegion region) {
        int regionSize = region.getSize();
        subpartitionFreeSpace[subpartition] -= regionSize;
        subpartitionCurrentOffset[subpartition] += regionSize;
        SegmentMeta segmentMeta = currentSegmentMeta[subpartition];
        // update min/max buffer index of segment.
        if (region.getFirstBufferIndex() < segmentMeta.getMinBufferIndex()) {
            segmentMeta.setMinBufferIndex(region.getFirstBufferIndex());
        }
        if (region.getFirstBufferIndex() + region.getNumBuffers()
                > segmentMeta.getMaxBufferIndex()) {
            segmentMeta.setMaxBufferIndex(region.getFirstBufferIndex() + region.getNumBuffers());
        }
        segmentMeta.addRegion();
    }

    /**
     * Read segment from index file.
     *
     * @param offset offset of this segment.
     * @param numRegions number of regions of this segment.
     * @return List of all regions and its offset belong to this segment.
     */
    private List<Tuple2<InternalRegion, Long>> readSegment(long offset, int numRegions)
            throws IOException {
        List<Tuple2<InternalRegion, Long>> regionAndOffsets = new ArrayList<>();
        for (int i = 0; i < numRegions; i++) {
            InternalRegion region =
                    InternalRegionWriteReadUtils.readRegionFromFile(
                            channel, immutablePartBuffer, offset);
            regionAndOffsets.add(Tuple2.of(region, offset));
            offset += region.getSize();
        }
        return regionAndOffsets;
    }

    /**
     * Metadata of spilled regions segment. When a segment is finished(i.e. no longer appended), its
     * corresponding {@link SegmentMeta} becomes immutable.
     */
    private static class SegmentMeta {
        /**
         * Minimum buffer index of this segment. It is the smallest bufferIndex(inclusive) in all
         * regions belong to this segment.
         */
        private int minBufferIndex;

        /**
         * Maximum buffer index of this segment. It is the largest bufferIndex(exclusive) in all
         * regions belong to this segment.
         */
        private int maxBufferIndex;

        /** Number of regions belong to this segment. */
        private int numRegions;

        /** The index file offset of this segment. */
        private final long offset;

        public SegmentMeta(long offset) {
            this.offset = offset;
            this.minBufferIndex = Integer.MAX_VALUE;
            this.maxBufferIndex = 0;
            this.numRegions = 0;
        }

        public int getMinBufferIndex() {
            return minBufferIndex;
        }

        public int getMaxBufferIndex() {
            return maxBufferIndex;
        }

        public long getOffset() {
            return offset;
        }

        public int getNumRegions() {
            return numRegions;
        }

        public void setMinBufferIndex(int minBufferIndex) {
            this.minBufferIndex = minBufferIndex;
        }

        public void setMaxBufferIndex(int maxBufferIndex) {
            this.maxBufferIndex = maxBufferIndex;
        }

        public void addRegion() {
            this.numRegions++;
        }
    }

    /** Factory of {@link HsFileDataIndexSpilledRegionManager}. */
    public static class Factory implements HsFileDataIndexSpilledRegionManager.Factory {
        public static final Factory INSTANCE = new Factory();

        @Override
        public HsFileDataIndexSpilledRegionManager create(
                int numSubpartitions,
                Path indexFilePath,
                int segmentSize,
                BiConsumer<Integer, InternalRegion> cacheRegionConsumer) {
            return new HsFileDataIndexSpilledRegionManagerImpl(
                    numSubpartitions, indexFilePath, segmentSize, cacheRegionConsumer);
        }
    }
}
