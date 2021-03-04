/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.VisibleForTesting;

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.WriteBufferManager;

/**
 * Utils to create {@link Cache} and {@link WriteBufferManager} which are used to control total
 * memory usage of RocksDB.
 */
public class RocksDBMemoryControllerUtils {

    /**
     * Allocate memory controllable RocksDB shared resources.
     *
     * @param totalMemorySize The total memory limit size.
     * @param writeBufferRatio The ratio of total memory which is occupied by write buffer manager.
     * @param highPriorityPoolRatio The high priority pool ratio of cache.
     * @return memory controllable RocksDB shared resources.
     */
    public static RocksDBSharedResources allocateRocksDBSharedResources(
            long totalMemorySize,
            double writeBufferRatio,
            double highPriorityPoolRatio,
            boolean usingPartitionedIndexFilters) {
        long calculatedCacheCapacity =
                RocksDBMemoryControllerUtils.calculateActualCacheCapacity(
                        totalMemorySize, writeBufferRatio);
        final Cache cache =
                RocksDBMemoryControllerUtils.createCache(
                        calculatedCacheCapacity, highPriorityPoolRatio);

        long writeBufferManagerCapacity =
                RocksDBMemoryControllerUtils.calculateWriteBufferManagerCapacity(
                        totalMemorySize, writeBufferRatio);
        final WriteBufferManager wbm =
                RocksDBMemoryControllerUtils.createWriteBufferManager(
                        writeBufferManagerCapacity, cache);

        return new RocksDBSharedResources(
                cache, wbm, writeBufferManagerCapacity, usingPartitionedIndexFilters);
    }

    /**
     * Calculate the actual memory capacity of cache, which would be shared among rocksDB
     * instance(s). We introduce this method because: a) We cannot create a strict capacity limit
     * cache util FLINK-15532 resolved. b) Regardless of the memory usage of blocks pinned by
     * RocksDB iterators, which is difficult to calculate and only happened when we iterator entries
     * in RocksDBMapState, the overuse of memory is mainly occupied by at most half of the write
     * buffer usage. (see <a
     * href="https://github.com/dataArtisans/frocksdb/blob/958f191d3f7276ae59b270f9db8390034d549ee0/include/rocksdb/write_buffer_manager.h#L51">the
     * flush implementation of write buffer manager</a>). Thus, we have four equations below:
     * write_buffer_manager_memory = 1.5 * write_buffer_manager_capacity write_buffer_manager_memory
     * = total_memory_size * write_buffer_ratio write_buffer_manager_memory + other_part =
     * total_memory_size write_buffer_manager_capacity + other_part = cache_capacity And we would
     * deduce the formula: cache_capacity = (3 - write_buffer_ratio) * total_memory_size / 3
     * write_buffer_manager_capacity = 2 * total_memory_size * write_buffer_ratio / 3
     *
     * @param totalMemorySize Total off-heap memory size reserved for RocksDB instance(s).
     * @param writeBufferRatio The ratio of total memory size which would be reserved for write
     *     buffer manager and its over-capacity part.
     * @return The actual calculated cache capacity.
     */
    @VisibleForTesting
    static long calculateActualCacheCapacity(long totalMemorySize, double writeBufferRatio) {
        return (long) ((3 - writeBufferRatio) * totalMemorySize / 3);
    }

    /**
     * Calculate the actual memory capacity of write buffer manager, which would be shared among
     * rocksDB instance(s). The formula to use here could refer to the doc of {@link
     * #calculateActualCacheCapacity(long, double)}.
     *
     * @param totalMemorySize Total off-heap memory size reserved for RocksDB instance(s).
     * @param writeBufferRatio The ratio of total memory size which would be reserved for write
     *     buffer manager and its over-capacity part.
     * @return The actual calculated write buffer manager capacity.
     */
    @VisibleForTesting
    static long calculateWriteBufferManagerCapacity(long totalMemorySize, double writeBufferRatio) {
        return (long) (2 * totalMemorySize * writeBufferRatio / 3);
    }

    @VisibleForTesting
    static Cache createCache(long cacheCapacity, double highPriorityPoolRatio) {
        // TODO use strict capacity limit until FLINK-15532 resolved
        return new LRUCache(cacheCapacity, -1, false, highPriorityPoolRatio);
    }

    @VisibleForTesting
    static WriteBufferManager createWriteBufferManager(
            long writeBufferManagerCapacity, Cache cache) {
        return new WriteBufferManager(writeBufferManagerCapacity, cache);
    }

    /**
     * Calculate the default arena block size as RocksDB calculates it in <a
     * href="https://github.com/dataArtisans/frocksdb/blob/49bc897d5d768026f1eb816d960c1f2383396ef4/db/column_family.cc#L196-L201">
     * here</a>.
     *
     * @return the default arena block size
     * @param writeBufferSize the write buffer size (bytes)
     */
    static long calculateRocksDBDefaultArenaBlockSize(long writeBufferSize) {
        long arenaBlockSize = writeBufferSize / 8;

        // Align up to 4k
        final long align = 4 * 1024;
        return ((arenaBlockSize + align - 1) / align) * align;
    }

    /**
     * Calculate {@code mutable_limit_} as RocksDB calculates it in <a
     * href="https://github.com/dataArtisans/frocksdb/blob/FRocksDB-5.17.2/memtable/write_buffer_manager.cc#L54">
     * here</a>.
     *
     * @param bufferSize write buffer size
     * @return mutableLimit
     */
    static long calculateRocksDBMutableLimit(long bufferSize) {
        return bufferSize * 7 / 8;
    }

    /**
     * RocksDB starts flushing the active memtable constantly in the case when the arena block size
     * is greater than mutable limit (as calculated in {@link #calculateRocksDBMutableLimit(long)}).
     *
     * <p>This happens because in such a case the check <a
     * href="https://github.com/dataArtisans/frocksdb/blob/958f191d3f7276ae59b270f9db8390034d549ee0/include/rocksdb/write_buffer_manager.h#L47">
     * here</a> is always true.
     *
     * <p>This method checks that arena block size is smaller than mutable limit.
     *
     * @param arenaBlockSize Arena block size
     * @param mutableLimit mutable limit
     * @return whether arena block size is sensible
     */
    @VisibleForTesting
    static boolean validateArenaBlockSize(long arenaBlockSize, long mutableLimit) {
        return arenaBlockSize <= mutableLimit;
    }
}
