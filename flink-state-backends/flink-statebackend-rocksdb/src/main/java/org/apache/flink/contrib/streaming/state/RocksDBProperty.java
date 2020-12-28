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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.Internal;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

/**
 * {@link RocksDB} properties that can be queried by Flink's metrics reporter.
 *
 * <p>Note: Metrics properties are added in each new version of {@link RocksDB}, when upgrading to a
 * latter version consider updating this class with newly added properties.
 */
@Internal
public enum RocksDBProperty {
    NumImmutableMemTable("num-immutable-mem-table"),
    MemTableFlushPending("mem-table-flush-pending"),
    CompactionPending("compaction-pending"),
    BackgroundErrors("background-errors"),
    CurSizeActiveMemTable("cur-size-active-mem-table"),
    CurSizeAllMemTables("cur-size-all-mem-tables"),
    SizeAllMemTables("size-all-mem-tables"),
    NumEntriesActiveMemTable("num-entries-active-mem-table"),
    NumEntriesImmMemTables("num-entries-imm-mem-tables"),
    NumDeletesActiveMemTable("num-deletes-active-mem-table"),
    NumDeletesImmMemTables("num-deletes-imm-mem-tables"),
    EstimateNumKeys("estimate-num-keys"),
    EstimateTableReadersMem("estimate-table-readers-mem"),
    NumSnapshots("num-snapshots"),
    NumLiveVersions("num-live-versions"),
    EstimateLiveDataSize("estimate-live-data-size"),
    TotalSstFilesSize("total-sst-files-size"),
    EstimatePendingCompactionBytes("estimate-pending-compaction-bytes"),
    NumRunningCompactions("num-running-compactions"),
    NumRunningFlushes("num-running-flushes"),
    ActualDelayedWriteRate("actual-delayed-write-rate"),
    IsWriteStopped("is-write-stopped"),
    BlockCacheCapacity("block-cache-capacity"),
    BlockCacheUsage("block-cache-usage"),
    BlockCachePinnedUsage("block-cache-pinned-usage");

    private static final String ROCKS_DB_PROPERTY_FORMAT = "rocksdb.%s";

    private static final String CONFIG_KEY_FORMAT = "state.backend.rocksdb.metrics.%s";

    private final String property;

    RocksDBProperty(String property) {
        this.property = property;
    }

    /**
     * @return property string that can be used to query {@link
     *     RocksDB#getLongProperty(ColumnFamilyHandle, String)}.
     */
    public String getRocksDBProperty() {
        return String.format(ROCKS_DB_PROPERTY_FORMAT, property);
    }

    /**
     * @return key for enabling metric using {@link org.apache.flink.configuration.Configuration}.
     */
    public String getConfigKey() {
        return String.format(CONFIG_KEY_FORMAT, property);
    }
}
