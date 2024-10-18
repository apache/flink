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

package org.apache.flink.state.rocksdb;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import org.rocksdb.TickerType;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Enable which RocksDB metrics to forward to Flink's metrics reporter.
 *
 * <p>Property based metrics would report at the column family level and return unsigned long
 * values.
 *
 * <p>Statistics based metrics would report at the database level, it can return ticker or histogram
 * kind results.
 *
 * <p>Properties and doc comments are taken from RocksDB documentation. See <a
 * href="https://github.com/facebook/rocksdb/blob/64324e329eb0a9b4e77241a425a1615ff524c7f1/include/rocksdb/db.h#L429">
 * db.h</a> for more information.
 */
@PublicEvolving
public class RocksDBNativeMetricOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    // --------------------------------------------------------------------------------------------
    //  RocksDB property based metrics, report at column family level
    // --------------------------------------------------------------------------------------------

    public static final String METRICS_COLUMN_FAMILY_AS_VARIABLE_KEY =
            "state.backend.rocksdb.metrics" + ".column-family-as-variable";

    public static final ConfigOption<Boolean> MONITOR_NUM_IMMUTABLE_MEM_TABLES =
            ConfigOptions.key(RocksDBProperty.NumImmutableMemTable.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the number of immutable memtables in RocksDB.");

    public static final ConfigOption<Boolean> MONITOR_MEM_TABLE_FLUSH_PENDING =
            ConfigOptions.key(RocksDBProperty.MemTableFlushPending.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the number of pending memtable flushes in RocksDB.");

    public static final ConfigOption<Boolean> TRACK_COMPACTION_PENDING =
            ConfigOptions.key(RocksDBProperty.CompactionPending.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Track pending compactions in RocksDB. Returns 1 if a compaction is pending, 0 otherwise.");

    public static final ConfigOption<Boolean> MONITOR_BACKGROUND_ERRORS =
            ConfigOptions.key(RocksDBProperty.BackgroundErrors.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the number of background errors in RocksDB.");

    public static final ConfigOption<Boolean> MONITOR_CUR_SIZE_ACTIVE_MEM_TABLE =
            ConfigOptions.key(RocksDBProperty.CurSizeActiveMemTable.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the approximate size of the active memtable in bytes.");

    public static final ConfigOption<Boolean> MONITOR_CUR_SIZE_ALL_MEM_TABLE =
            ConfigOptions.key(RocksDBProperty.CurSizeAllMemTables.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the approximate size of the active and unflushed immutable memtables"
                                    + " in bytes.");

    public static final ConfigOption<Boolean> MONITOR_SIZE_ALL_MEM_TABLES =
            ConfigOptions.key(RocksDBProperty.SizeAllMemTables.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the approximate size of the active, unflushed immutable, "
                                    + "and pinned immutable memtables in bytes.");

    public static final ConfigOption<Boolean> MONITOR_NUM_ENTRIES_ACTIVE_MEM_TABLE =
            ConfigOptions.key(RocksDBProperty.NumEntriesActiveMemTable.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the total number of entries in the active memtable.");

    public static final ConfigOption<Boolean> MONITOR_NUM_ENTRIES_IMM_MEM_TABLES =
            ConfigOptions.key(RocksDBProperty.NumEntriesImmMemTables.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the total number of entries in the unflushed immutable memtables.");

    public static final ConfigOption<Boolean> MONITOR_NUM_DELETES_ACTIVE_MEM_TABLE =
            ConfigOptions.key(RocksDBProperty.NumDeletesActiveMemTable.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the total number of delete entries in the active memtable.");

    public static final ConfigOption<Boolean> MONITOR_NUM_DELETES_IMM_MEM_TABLE =
            ConfigOptions.key(RocksDBProperty.NumDeletesImmMemTables.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the total number of delete entries in the unflushed immutable memtables.");

    public static final ConfigOption<Boolean> ESTIMATE_NUM_KEYS =
            ConfigOptions.key(RocksDBProperty.EstimateNumKeys.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Estimate the number of keys in RocksDB.");

    public static final ConfigOption<Boolean> ESTIMATE_TABLE_READERS_MEM =
            ConfigOptions.key(RocksDBProperty.EstimateTableReadersMem.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Estimate the memory used for reading SST tables, excluding memory"
                                    + " used in block cache (e.g.,filter and index blocks) in bytes.");

    public static final ConfigOption<Boolean> MONITOR_NUM_SNAPSHOTS =
            ConfigOptions.key(RocksDBProperty.NumSnapshots.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the number of unreleased snapshots of the database.");

    public static final ConfigOption<Boolean> MONITOR_NUM_LIVE_VERSIONS =
            ConfigOptions.key(RocksDBProperty.NumLiveVersions.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor number of live versions. Version is an internal data structure. "
                                    + "See RocksDB file version_set.h for details. More live versions often mean more SST files are held "
                                    + "from being deleted, by iterators or unfinished compactions.");

    public static final ConfigOption<Boolean> ESTIMATE_LIVE_DATA_SIZE =
            ConfigOptions.key(RocksDBProperty.EstimateLiveDataSize.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Estimate of the amount of live data in bytes (usually smaller than sst files size due to space amplification).");

    public static final ConfigOption<Boolean> MONITOR_TOTAL_SST_FILES_SIZE =
            ConfigOptions.key(RocksDBProperty.TotalSstFilesSize.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the total size (bytes) of all SST files of all versions."
                                    + "WARNING: may slow down online queries if there are too many files.");

    public static final ConfigOption<Boolean> MONITOR_LIVE_SST_FILES_SIZE =
            ConfigOptions.key(RocksDBProperty.LiveSstFilesSize.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the total size (bytes) of all SST files belonging to the latest version."
                                    + "WARNING: may slow down online queries if there are too many files.");

    public static final ConfigOption<Boolean> ESTIMATE_PENDING_COMPACTION_BYTES =
            ConfigOptions.key(RocksDBProperty.EstimatePendingCompactionBytes.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Estimated total number of bytes compaction needs to rewrite to get all levels "
                                    + "down to under target size. Not valid for other compactions than level-based.");

    public static final ConfigOption<Boolean> MONITOR_NUM_RUNNING_COMPACTIONS =
            ConfigOptions.key(RocksDBProperty.NumRunningCompactions.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the number of currently running compactions.");

    public static final ConfigOption<Boolean> MONITOR_NUM_RUNNING_FLUSHES =
            ConfigOptions.key(RocksDBProperty.NumRunningFlushes.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the number of currently running flushes.");

    public static final ConfigOption<Boolean> MONITOR_ACTUAL_DELAYED_WRITE_RATE =
            ConfigOptions.key(RocksDBProperty.ActualDelayedWriteRate.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the current actual delayed write rate. 0 means no delay.");

    public static final ConfigOption<Boolean> IS_WRITE_STOPPED =
            ConfigOptions.key(RocksDBProperty.IsWriteStopped.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Track whether write has been stopped in RocksDB. Returns 1 if write has been stopped, 0 otherwise.");

    public static final ConfigOption<Boolean> BLOCK_CACHE_CAPACITY =
            ConfigOptions.key(RocksDBProperty.BlockCacheCapacity.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor block cache capacity.");

    public static final ConfigOption<Boolean> BLOCK_CACHE_USAGE =
            ConfigOptions.key(RocksDBProperty.BlockCacheUsage.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the memory size for the entries residing in block cache.");

    public static final ConfigOption<Boolean> BLOCK_CACHE_PINNED_USAGE =
            ConfigOptions.key(RocksDBProperty.BlockCachePinnedUsage.getConfigKey())
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the memory size for the entries being pinned in block cache.");

    public static final ConfigOption<Boolean> COLUMN_FAMILY_AS_VARIABLE =
            ConfigOptions.key(METRICS_COLUMN_FAMILY_AS_VARIABLE_KEY)
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to expose the column family as a variable for RocksDB property based metrics.");

    public static final ConfigOption<Boolean> MONITOR_NUM_FILES_AT_LEVEL =
            ConfigOptions.key("state.backend.rocksdb.metrics.num-files-at-level")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the number of files at each level.");

    // --------------------------------------------------------------------------------------------
    //  RocksDB statistics based metrics, report at database level
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<Boolean> MONITOR_BLOCK_CACHE_HIT =
            ConfigOptions.key("state.backend.rocksdb.metrics.block-cache-hit")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the total count of block cache hit in RocksDB (BLOCK_CACHE_HIT == BLOCK_CACHE_INDEX_HIT + BLOCK_CACHE_FILTER_HIT + BLOCK_CACHE_DATA_HIT).");

    public static final ConfigOption<Boolean> MONITOR_BLOCK_CACHE_MISS =
            ConfigOptions.key("state.backend.rocksdb.metrics.block-cache-miss")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the total count of block cache misses in RocksDB (BLOCK_CACHE_MISS == BLOCK_CACHE_INDEX_MISS + BLOCK_CACHE_FILTER_MISS + BLOCK_CACHE_DATA_MISS).");

    public static final ConfigOption<Boolean> MONITOR_BLOOM_FILTER_USEFUL =
            ConfigOptions.key("state.backend.rocksdb.metrics.bloom-filter-useful")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the total count of reads avoided by bloom filter.");

    public static final ConfigOption<Boolean> MONITOR_BLOOM_FILTER_FULL_POSITIVE =
            ConfigOptions.key("state.backend.rocksdb.metrics.bloom-filter-full-positive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the total count of reads not avoided by bloom full filter.");

    public static final ConfigOption<Boolean> MONITOR_BLOOM_FILTER_FULL_TRUE_POSITIVE =
            ConfigOptions.key("state.backend.rocksdb.metrics.bloom-filter-full-true-positive")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the total count of reads not avoided by bloom full filter and the data actually exists in RocksDB.");

    public static final ConfigOption<Boolean> MONITOR_BYTES_READ =
            ConfigOptions.key("state.backend.rocksdb.metrics.bytes-read")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the number of uncompressed bytes read (from memtables/cache/sst) from Get() operation in RocksDB.");

    public static final ConfigOption<Boolean> MONITOR_ITER_BYTES_READ =
            ConfigOptions.key("state.backend.rocksdb.metrics.iter-bytes-read")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the number of uncompressed bytes read (from memtables/cache/sst) from an iterator operation in RocksDB.");

    public static final ConfigOption<Boolean> MONITOR_BYTES_WRITTEN =
            ConfigOptions.key("state.backend.rocksdb.metrics.bytes-written")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the number of uncompressed bytes written by DB::{Put(), Delete(), Merge(), Write()} operations, which does not include the compaction written bytes, in RocksDB.");

    public static final ConfigOption<Boolean> MONITOR_COMPACTION_READ_BYTES =
            ConfigOptions.key("state.backend.rocksdb.metrics.compaction-read-bytes")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the bytes read during compaction in RocksDB.");

    public static final ConfigOption<Boolean> MONITOR_COMPACTION_WRITE_BYTES =
            ConfigOptions.key("state.backend.rocksdb.metrics.compaction-write-bytes")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Monitor the bytes written during compaction in RocksDB.");

    public static final ConfigOption<Boolean> MONITOR_STALL_MICROS =
            ConfigOptions.key("state.backend.rocksdb.metrics.stall-micros")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Monitor the duration of writer requiring to wait for compaction or flush to finish in RocksDB.");

    /** Creates a {@link RocksDBNativeMetricOptions} based on an external configuration. */
    public static RocksDBNativeMetricOptions fromConfig(ReadableConfig config) {
        RocksDBNativeMetricOptions options = new RocksDBNativeMetricOptions();
        configurePropertyMetrics(options, config);
        configureStatisticsMetrics(options, config);
        return options;
    }

    private static void configurePropertyMetrics(
            RocksDBNativeMetricOptions options, ReadableConfig config) {
        if (config.get(MONITOR_NUM_IMMUTABLE_MEM_TABLES)) {
            options.enableNumImmutableMemTable();
        }

        if (config.get(MONITOR_MEM_TABLE_FLUSH_PENDING)) {
            options.enableMemTableFlushPending();
        }

        if (config.get(TRACK_COMPACTION_PENDING)) {
            options.enableCompactionPending();
        }

        if (config.get(MONITOR_BACKGROUND_ERRORS)) {
            options.enableBackgroundErrors();
        }

        if (config.get(MONITOR_CUR_SIZE_ACTIVE_MEM_TABLE)) {
            options.enableCurSizeActiveMemTable();
        }

        if (config.get(MONITOR_CUR_SIZE_ALL_MEM_TABLE)) {
            options.enableCurSizeAllMemTables();
        }

        if (config.get(MONITOR_SIZE_ALL_MEM_TABLES)) {
            options.enableSizeAllMemTables();
        }

        if (config.get(MONITOR_NUM_ENTRIES_ACTIVE_MEM_TABLE)) {
            options.enableNumEntriesActiveMemTable();
        }

        if (config.get(MONITOR_NUM_ENTRIES_IMM_MEM_TABLES)) {
            options.enableNumEntriesImmMemTables();
        }

        if (config.get(MONITOR_NUM_DELETES_ACTIVE_MEM_TABLE)) {
            options.enableNumDeletesActiveMemTable();
        }

        if (config.get(MONITOR_NUM_DELETES_IMM_MEM_TABLE)) {
            options.enableNumDeletesImmMemTables();
        }

        if (config.get(ESTIMATE_NUM_KEYS)) {
            options.enableEstimateNumKeys();
        }

        if (config.get(ESTIMATE_TABLE_READERS_MEM)) {
            options.enableEstimateTableReadersMem();
        }

        if (config.get(MONITOR_NUM_SNAPSHOTS)) {
            options.enableNumSnapshots();
        }

        if (config.get(MONITOR_NUM_LIVE_VERSIONS)) {
            options.enableNumLiveVersions();
        }

        if (config.get(ESTIMATE_LIVE_DATA_SIZE)) {
            options.enableEstimateLiveDataSize();
        }

        if (config.get(MONITOR_TOTAL_SST_FILES_SIZE)) {
            options.enableTotalSstFilesSize();
        }

        if (config.get(MONITOR_LIVE_SST_FILES_SIZE)) {
            options.enableLiveSstFilesSize();
        }

        if (config.get(ESTIMATE_PENDING_COMPACTION_BYTES)) {
            options.enableEstimatePendingCompactionBytes();
        }

        if (config.get(MONITOR_NUM_RUNNING_COMPACTIONS)) {
            options.enableNumRunningCompactions();
        }

        if (config.get(MONITOR_NUM_RUNNING_FLUSHES)) {
            options.enableNumRunningFlushes();
        }

        if (config.get(MONITOR_ACTUAL_DELAYED_WRITE_RATE)) {
            options.enableActualDelayedWriteRate();
        }

        if (config.get(IS_WRITE_STOPPED)) {
            options.enableIsWriteStopped();
        }

        if (config.get(BLOCK_CACHE_CAPACITY)) {
            options.enableBlockCacheCapacity();
        }

        if (config.get(BLOCK_CACHE_USAGE)) {
            options.enableBlockCacheUsage();
        }

        if (config.get(BLOCK_CACHE_PINNED_USAGE)) {
            options.enableBlockCachePinnedUsage();
        }

        if (config.get(MONITOR_NUM_FILES_AT_LEVEL)) {
            options.enableNumFilesAtLevel();
        }

        options.setColumnFamilyAsVariable(config.get(COLUMN_FAMILY_AS_VARIABLE));
    }

    private static void configureStatisticsMetrics(
            RocksDBNativeMetricOptions options, ReadableConfig config) {
        for (Map.Entry<ConfigOption<Boolean>, TickerType> entry : tickerTypeMapping.entrySet()) {
            if (config.get(entry.getKey())) {
                options.monitorTickerTypes.add(entry.getValue());
            }
        }
    }

    private static final Map<ConfigOption<Boolean>, TickerType> tickerTypeMapping =
            new HashMap<ConfigOption<Boolean>, TickerType>() {
                private static final long serialVersionUID = 1L;

                {
                    put(MONITOR_BLOCK_CACHE_HIT, TickerType.BLOCK_CACHE_HIT);
                    put(MONITOR_BLOCK_CACHE_MISS, TickerType.BLOCK_CACHE_MISS);
                    put(MONITOR_BLOOM_FILTER_USEFUL, TickerType.BLOOM_FILTER_USEFUL);
                    put(MONITOR_BLOOM_FILTER_FULL_POSITIVE, TickerType.BLOOM_FILTER_FULL_POSITIVE);
                    put(
                            MONITOR_BLOOM_FILTER_FULL_TRUE_POSITIVE,
                            TickerType.BLOOM_FILTER_FULL_TRUE_POSITIVE);
                    put(MONITOR_BYTES_READ, TickerType.BYTES_READ);
                    put(MONITOR_ITER_BYTES_READ, TickerType.ITER_BYTES_READ);
                    put(MONITOR_BYTES_WRITTEN, TickerType.BYTES_WRITTEN);
                    put(MONITOR_COMPACTION_READ_BYTES, TickerType.COMPACT_READ_BYTES);
                    put(MONITOR_COMPACTION_WRITE_BYTES, TickerType.COMPACT_WRITE_BYTES);
                    put(MONITOR_STALL_MICROS, TickerType.STALL_MICROS);
                }
            };

    private final Set<RocksDBProperty> properties;
    private final Set<TickerType> monitorTickerTypes;
    private boolean columnFamilyAsVariable = COLUMN_FAMILY_AS_VARIABLE.defaultValue();

    public RocksDBNativeMetricOptions() {
        this.properties = new HashSet<>();
        this.monitorTickerTypes = new HashSet<>();
    }

    @VisibleForTesting
    public void enableNativeStatistics(ConfigOption<Boolean> nativeStatisticsOption) {
        TickerType tickerType = tickerTypeMapping.get(nativeStatisticsOption);
        if (tickerType != null) {
            monitorTickerTypes.add(tickerType);
        } else {
            throw new IllegalArgumentException(
                    "Unknown configurable native statistics option " + nativeStatisticsOption);
        }
    }

    /** Returns number of immutable memtables that have not yet been flushed. */
    public void enableNumImmutableMemTable() {
        this.properties.add(RocksDBProperty.NumImmutableMemTable);
    }

    /** Returns 1 if a memtable flush is pending; otherwise, returns 0. */
    public void enableMemTableFlushPending() {
        this.properties.add(RocksDBProperty.MemTableFlushPending);
    }

    /** Returns 1 if at least one compaction is pending; otherwise, returns 0. */
    public void enableCompactionPending() {
        this.properties.add(RocksDBProperty.CompactionPending);
    }

    /** Returns accumulated number of background errors. */
    public void enableBackgroundErrors() {
        this.properties.add(RocksDBProperty.BackgroundErrors);
    }

    /** Returns approximate size of active memtable (bytes). */
    public void enableCurSizeActiveMemTable() {
        this.properties.add(RocksDBProperty.CurSizeActiveMemTable);
    }

    /** Returns approximate size of active and unflushed immutable memtables (bytes). */
    public void enableCurSizeAllMemTables() {
        this.properties.add(RocksDBProperty.CurSizeAllMemTables);
    }

    /**
     * Returns approximate size of active, unflushed immutable, and pinned immutable memtables
     * (bytes).
     */
    public void enableSizeAllMemTables() {
        this.properties.add(RocksDBProperty.SizeAllMemTables);
    }

    /** Returns total number of entries in the active memtable. */
    public void enableNumEntriesActiveMemTable() {
        this.properties.add(RocksDBProperty.NumEntriesActiveMemTable);
    }

    /** Returns total number of entries in the unflushed immutable memtables. */
    public void enableNumEntriesImmMemTables() {
        this.properties.add(RocksDBProperty.NumEntriesImmMemTables);
    }

    /** Returns total number of delete entries in the active memtable. */
    public void enableNumDeletesActiveMemTable() {
        this.properties.add(RocksDBProperty.NumDeletesActiveMemTable);
    }

    /** Returns total number of delete entries in the unflushed immutable memtables. */
    public void enableNumDeletesImmMemTables() {
        this.properties.add(RocksDBProperty.NumDeletesImmMemTables);
    }

    /**
     * Returns estimated number of total keys in the active and unflushed immutable memtables and
     * storage.
     */
    public void enableEstimateNumKeys() {
        this.properties.add(RocksDBProperty.EstimateNumKeys);
    }

    /**
     * Returns estimated memory used for reading SST tables, excluding memory used in block cache
     * (e.g.,filter and index blocks).
     */
    public void enableEstimateTableReadersMem() {
        this.properties.add(RocksDBProperty.EstimateTableReadersMem);
    }

    /** Returns number of unreleased snapshots of the database. */
    public void enableNumSnapshots() {
        this.properties.add(RocksDBProperty.NumSnapshots);
    }

    /**
     * Returns number of live versions. `Version` is an internal data structure. See version_set.h
     * for details. More live versions often mean more SST files are held from being deleted, by
     * iterators or unfinished compactions.
     */
    public void enableNumLiveVersions() {
        this.properties.add(RocksDBProperty.NumLiveVersions);
    }

    /** Returns an estimate of the amount of live data in bytes. */
    public void enableEstimateLiveDataSize() {
        this.properties.add(RocksDBProperty.EstimateLiveDataSize);
    }

    /**
     * Returns total size (bytes) of all SST files. <strong>WARNING</strong>: may slow down online
     * queries if there are too many files.
     */
    public void enableTotalSstFilesSize() {
        this.properties.add(RocksDBProperty.TotalSstFilesSize);
    }

    public void enableLiveSstFilesSize() {
        this.properties.add(RocksDBProperty.LiveSstFilesSize);
    }

    /**
     * Returns estimated total number of bytes compaction needs to rewrite to get all levels down to
     * under target size. Not valid for other compactions than level-based.
     */
    public void enableEstimatePendingCompactionBytes() {
        this.properties.add(RocksDBProperty.EstimatePendingCompactionBytes);
    }

    /** Returns the number of currently running compactions. */
    public void enableNumRunningCompactions() {
        this.properties.add(RocksDBProperty.NumRunningCompactions);
    }

    /** Returns the number of currently running flushes. */
    public void enableNumRunningFlushes() {
        this.properties.add(RocksDBProperty.NumRunningFlushes);
    }

    /** Returns the current actual delayed write rate. 0 means no delay. */
    public void enableActualDelayedWriteRate() {
        this.properties.add(RocksDBProperty.ActualDelayedWriteRate);
    }

    /** Returns 1 if write has been stopped. */
    public void enableIsWriteStopped() {
        this.properties.add(RocksDBProperty.IsWriteStopped);
    }

    /** Returns block cache capacity. */
    public void enableBlockCacheCapacity() {
        this.properties.add(RocksDBProperty.BlockCacheCapacity);
    }

    /** Returns the memory size for the entries residing in block cache. */
    public void enableBlockCacheUsage() {
        this.properties.add(RocksDBProperty.BlockCacheUsage);
    }

    /** Returns the memory size for the entries being pinned in block cache. */
    public void enableBlockCachePinnedUsage() {
        this.properties.add(RocksDBProperty.BlockCachePinnedUsage);
    }

    /** Returns the number of files per level. */
    public void enableNumFilesAtLevel() {
        this.properties.add(RocksDBProperty.NumFilesAtLevel0);
        this.properties.add(RocksDBProperty.NumFilesAtLevel1);
        this.properties.add(RocksDBProperty.NumFilesAtLevel2);
        this.properties.add(RocksDBProperty.NumFilesAtLevel3);
        this.properties.add(RocksDBProperty.NumFilesAtLevel4);
        this.properties.add(RocksDBProperty.NumFilesAtLevel5);
        this.properties.add(RocksDBProperty.NumFilesAtLevel6);
    }

    /** Returns the column family as variable. */
    public void setColumnFamilyAsVariable(boolean columnFamilyAsVariable) {
        this.columnFamilyAsVariable = columnFamilyAsVariable;
    }

    /** @return the enabled RocksDB property-based metrics */
    public Collection<RocksDBProperty> getProperties() {
        return Collections.unmodifiableCollection(properties);
    }

    /** @return the enabled RocksDB statistics metrics. */
    public Collection<TickerType> getMonitorTickerTypes() {
        return Collections.unmodifiableCollection(monitorTickerTypes);
    }

    /**
     * {{@link RocksDBNativeMetricMonitor}} is enabled if any property or ticker type is set.
     *
     * @return true if {{RocksDBNativeMetricMonitor}} should be enabled, false otherwise.
     */
    public boolean isEnabled() {
        return !properties.isEmpty() || isStatisticsEnabled();
    }

    /** @return true if RocksDB statistics metrics are enabled, false otherwise. */
    public boolean isStatisticsEnabled() {
        return !monitorTickerTypes.isEmpty();
    }

    /**
     * {{@link RocksDBNativeMetricMonitor}} Whether to expose the column family as a variable..
     *
     * @return true is column family to expose variable, false otherwise.
     */
    public boolean isColumnFamilyAsVariable() {
        return this.columnFamilyAsVariable;
    }
}
