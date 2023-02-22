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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;

import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava30.com.google.common.cache.RemovalNotification;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A cache layer of hybrid data index. This class encapsulates the logic of the index's put and get,
 * and automatically caches some indexes in memory. When there are too many cached indexes, it is
 * this class's responsibility to decide and eliminate some indexes to disk.
 */
public class HsFileDataIndexCache {
    /**
     * This struct stores all in memory {@link InternalRegion}s. Each element is a treeMap contains
     * all in memory {@link InternalRegion}'s of specific subpartition corresponding to the
     * subscript. The value of this treeMap is a {@link InternalRegion}, and the key is
     * firstBufferIndex of this region. Only cached in memory region will be put to here.
     */
    private final List<TreeMap<Integer, InternalRegion>>
            subpartitionFirstBufferIndexInternalRegions;

    /**
     * This cache is used to help eliminate regions from memory. It is only maintains the key of
     * each in memory region, the value is just a placeholder. Note that this internal cache must be
     * consistent with subpartitionFirstBufferIndexInternalRegions, that means both of them must add
     * or delete elements at the same time.
     */
    private final Cache<CachedRegionKey, Object> internalCache;

    private final HsFileDataIndexSpilledRegionManager spilledRegionManager;

    private final Path indexFilePath;

    /**
     * Placeholder of cache entry's value. Because the cache is only used for managing region's
     * elimination, does not need the real region as value.
     */
    public static final Object PLACEHOLDER = new Object();

    public HsFileDataIndexCache(
            int numSubpartitions,
            Path indexFilePath,
            long numRetainedInMemoryRegionsMax,
            HsFileDataIndexSpilledRegionManager.Factory spilledRegionManagerFactory) {
        this.subpartitionFirstBufferIndexInternalRegions = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionFirstBufferIndexInternalRegions.add(new TreeMap<>());
        }
        this.internalCache =
                CacheBuilder.newBuilder()
                        .maximumSize(numRetainedInMemoryRegionsMax)
                        .removalListener(this::handleRemove)
                        .build();
        this.indexFilePath = checkNotNull(indexFilePath);
        this.spilledRegionManager =
                spilledRegionManagerFactory.create(
                        numSubpartitions,
                        indexFilePath,
                        (subpartition, region) -> {
                            if (!getCachedRegionContainsTargetBufferIndex(
                                            subpartition, region.getFirstBufferIndex())
                                    .isPresent()) {
                                subpartitionFirstBufferIndexInternalRegions
                                        .get(subpartition)
                                        .put(region.getFirstBufferIndex(), region);
                                internalCache.put(
                                        new CachedRegionKey(
                                                subpartition, region.getFirstBufferIndex()),
                                        PLACEHOLDER);
                            } else {
                                // this is needed for cache entry remove algorithm like LRU.
                                internalCache.getIfPresent(
                                        new CachedRegionKey(
                                                subpartition, region.getFirstBufferIndex()));
                            }
                        });
    }

    /**
     * Get a region contains target bufferIndex and belong to target subpartition.
     *
     * @param subpartitionId the subpartition that target buffer belong to.
     * @param bufferIndex the index of target buffer.
     * @return If target region can be founded from memory or disk, return optional contains target
     *     region. Otherwise, return {@code Optional#empty()};
     */
    public Optional<InternalRegion> get(int subpartitionId, int bufferIndex) {
        // first of all, try to get region in memory.
        Optional<InternalRegion> regionOpt =
                getCachedRegionContainsTargetBufferIndex(subpartitionId, bufferIndex);
        if (regionOpt.isPresent()) {
            InternalRegion region = regionOpt.get();
            checkNotNull(
                    // this is needed for cache entry remove algorithm like LRU.
                    internalCache.getIfPresent(
                            new CachedRegionKey(subpartitionId, region.getFirstBufferIndex())));
            return Optional.of(region);
        } else {
            // try to find target region and load it into cache if founded.
            spilledRegionManager.findRegion(subpartitionId, bufferIndex, true);
            return getCachedRegionContainsTargetBufferIndex(subpartitionId, bufferIndex);
        }
    }

    /**
     * Put regions to cache.
     *
     * @param subpartition the subpartition's id of regions.
     * @param internalRegions regions to be cached.
     */
    public void put(int subpartition, List<InternalRegion> internalRegions) {
        TreeMap<Integer, InternalRegion> treeMap =
                subpartitionFirstBufferIndexInternalRegions.get(subpartition);
        for (InternalRegion internalRegion : internalRegions) {
            internalCache.put(
                    new CachedRegionKey(subpartition, internalRegion.getFirstBufferIndex()),
                    PLACEHOLDER);
            treeMap.put(internalRegion.getFirstBufferIndex(), internalRegion);
        }
    }

    /**
     * Close {@link HsFileDataIndexCache}, this will delete the index file. After that, the index
     * can no longer be read or written.
     */
    public void close() throws IOException {
        spilledRegionManager.close();
        IOUtils.deleteFileQuietly(indexFilePath);
    }

    // This is a callback after internal cache removed an entry from itself.
    private void handleRemove(RemovalNotification<CachedRegionKey, Object> removedEntry) {
        CachedRegionKey removedKey = removedEntry.getKey();
        // remove the corresponding region from memory.
        InternalRegion removedRegion =
                subpartitionFirstBufferIndexInternalRegions
                        .get(removedKey.getSubpartition())
                        .remove(removedKey.getFirstBufferIndex());

        // write this region to file. After that, no strong reference point to this region, it can
        // be safely released by gc.
        writeRegion(removedKey.getSubpartition(), removedRegion);
    }

    private void writeRegion(int subpartition, InternalRegion region) {
        try {
            spilledRegionManager.appendOrOverwriteRegion(subpartition, region);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /**
     * Get the cached in memory region contains target buffer.
     *
     * @param subpartitionId the subpartition that target buffer belong to.
     * @param bufferIndex the index of target buffer.
     * @return If target region is cached in memory, return optional contains target region.
     *     Otherwise, return {@code Optional#empty()};
     */
    private Optional<InternalRegion> getCachedRegionContainsTargetBufferIndex(
            int subpartitionId, int bufferIndex) {
        return Optional.ofNullable(
                        subpartitionFirstBufferIndexInternalRegions
                                .get(subpartitionId)
                                .floorEntry(bufferIndex))
                .map(Map.Entry::getValue)
                .filter(internalRegion -> internalRegion.containBuffer(bufferIndex));
    }

    /**
     * This class represents the key of cached region, it is uniquely identified by the region's
     * subpartition id and firstBufferIndex.
     */
    private static class CachedRegionKey {
        /** The subpartition id of cached region. */
        private final int subpartition;

        /** The first buffer's index of cached region. */
        private final int firstBufferIndex;

        public CachedRegionKey(int subpartition, int firstBufferIndex) {
            this.subpartition = subpartition;
            this.firstBufferIndex = firstBufferIndex;
        }

        public int getSubpartition() {
            return subpartition;
        }

        public int getFirstBufferIndex() {
            return firstBufferIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CachedRegionKey that = (CachedRegionKey) o;
            return subpartition == that.subpartition && firstBufferIndex == that.firstBufferIndex;
        }

        @Override
        public int hashCode() {
            return Objects.hash(subpartition, firstBufferIndex);
        }
    }
}
