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

package org.apache.flink.state.rocksdb;

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
    NumImmutableMemTable("num-immutable-mem-table", PropertyType.NUMBER),
    MemTableFlushPending("mem-table-flush-pending", PropertyType.NUMBER),
    CompactionPending("compaction-pending", PropertyType.NUMBER),
    BackgroundErrors("background-errors", PropertyType.NUMBER),
    CurSizeActiveMemTable("cur-size-active-mem-table", PropertyType.NUMBER),
    CurSizeAllMemTables("cur-size-all-mem-tables", PropertyType.NUMBER),
    SizeAllMemTables("size-all-mem-tables", PropertyType.NUMBER),
    NumEntriesActiveMemTable("num-entries-active-mem-table", PropertyType.NUMBER),
    NumEntriesImmMemTables("num-entries-imm-mem-tables", PropertyType.NUMBER),
    NumDeletesActiveMemTable("num-deletes-active-mem-table", PropertyType.NUMBER),
    NumDeletesImmMemTables("num-deletes-imm-mem-tables", PropertyType.NUMBER),
    EstimateNumKeys("estimate-num-keys", PropertyType.NUMBER),
    EstimateTableReadersMem("estimate-table-readers-mem", PropertyType.NUMBER),
    NumSnapshots("num-snapshots", PropertyType.NUMBER),
    NumLiveVersions("num-live-versions", PropertyType.NUMBER),
    EstimateLiveDataSize("estimate-live-data-size", PropertyType.NUMBER),
    TotalSstFilesSize("total-sst-files-size", PropertyType.NUMBER),
    LiveSstFilesSize("live-sst-files-size", PropertyType.NUMBER),
    EstimatePendingCompactionBytes("estimate-pending-compaction-bytes", PropertyType.NUMBER),
    NumRunningCompactions("num-running-compactions", PropertyType.NUMBER),
    NumRunningFlushes("num-running-flushes", PropertyType.NUMBER),
    ActualDelayedWriteRate("actual-delayed-write-rate", PropertyType.NUMBER),
    IsWriteStopped("is-write-stopped", PropertyType.NUMBER),
    BlockCacheCapacity("block-cache-capacity", PropertyType.NUMBER),
    BlockCacheUsage("block-cache-usage", PropertyType.NUMBER),
    BlockCachePinnedUsage("block-cache-pinned-usage", PropertyType.NUMBER),
    NumFilesAtLevel0("num-files-at-level0", PropertyType.STRING),
    NumFilesAtLevel1("num-files-at-level1", PropertyType.STRING),
    NumFilesAtLevel2("num-files-at-level2", PropertyType.STRING),
    NumFilesAtLevel3("num-files-at-level3", PropertyType.STRING),
    NumFilesAtLevel4("num-files-at-level4", PropertyType.STRING),
    NumFilesAtLevel5("num-files-at-level5", PropertyType.STRING),
    NumFilesAtLevel6("num-files-at-level6", PropertyType.STRING);

    private static final String ROCKS_DB_PROPERTY_FORMAT = "rocksdb.%s";

    protected static final String CONFIG_KEY_FORMAT = "state.backend.rocksdb.metrics.%s";

    private final String property;

    private final PropertyType type;

    /** Property type. */
    private enum PropertyType {
        NUMBER,
        STRING
    }

    RocksDBProperty(String property, PropertyType type) {
        this.property = property;
        this.type = type;
    }

    /**
     * @return property string that can be used to query {@link
     *     RocksDB#getLongProperty(ColumnFamilyHandle, String)}.
     */
    public String getRocksDBProperty() {
        return String.format(ROCKS_DB_PROPERTY_FORMAT, property);
    }

    public long getNumericalPropertyValue(RocksDB rocksDB, ColumnFamilyHandle handle)
            throws Exception {
        String rocksDBProperty = getRocksDBProperty();
        switch (type) {
            case NUMBER:
                return rocksDB.getLongProperty(handle, rocksDBProperty);
            case STRING:
                return Long.parseLong(rocksDB.getProperty(handle, rocksDBProperty));
            default:
                throw new RuntimeException(
                        String.format("RocksDB property type: %s not supported", type));
        }
    }

    /**
     * @return key for enabling metric using {@link org.apache.flink.configuration.Configuration}.
     */
    public String getConfigKey() {
        return String.format(CONFIG_KEY_FORMAT, property);
    }
}
