/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.CollectionUtil;

import org.rocksdb.CompactionStyle;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@code PredefinedOptions} are configuration settings for the {@link
 * EmbeddedRocksDBStateBackend}. The various pre-defined choices are configurations that have been
 * empirically determined to be beneficial for performance under different settings.
 *
 * <p>Some of these settings are based on experiments by the Flink community, some follow guides
 * from the RocksDB project.
 *
 * <p>All of them effectively disable the RocksDB log by default because this file would grow
 * indefinitely and will be deleted with the TM anyway.
 *
 * <p>The {@code PredefinedOptions} are designed to cope with different situations. If some
 * configurations should be enabled unconditionally, they are not included in any of the pre-defined
 * options. Please check {@link RocksDBResourceContainer#createBaseCommonDBOptions()} and {@link
 * RocksDBResourceContainer#createBaseCommonColumnOptions()} for common settings. Note that
 * setUseFsync(false) is set by default irrespective of the {@code PredefinedOptions} setting.
 * Because Flink does not rely on RocksDB data on disk for recovery, there is no need to sync data
 * to stable storage.
 */
public enum PredefinedOptions {

    /**
     * Default options for all settings.
     *
     * <p>There are no specified options here.
     */
    DEFAULT(Collections.emptyMap()),

    /**
     * Pre-defined options for regular spinning hard disks.
     *
     * <p>This constant configures RocksDB with some options that lead empirically to better
     * performance when the machines executing the system use regular spinning hard disks.
     *
     * <p>The following options are set:
     *
     * <ul>
     *   <li>setCompactionStyle(CompactionStyle.LEVEL)
     *   <li>setLevelCompactionDynamicLevelBytes(true)
     *   <li>setMaxBackgroundJobs(4)
     *   <li>setMaxOpenFiles(-1)
     * </ul>
     */
    SPINNING_DISK_OPTIMIZED(
            new HashMap<ConfigOption<?>, Object>() {
                private static final long serialVersionUID = 1L;

                {
                    put(RocksDBConfigurableOptions.COMPACTION_STYLE, CompactionStyle.LEVEL);
                    put(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, true);
                    put(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 4);
                    put(RocksDBConfigurableOptions.MAX_OPEN_FILES, -1);
                }
            }),

    /**
     * Pre-defined options for better performance on regular spinning hard disks, at the cost of a
     * higher memory consumption.
     *
     * <p><b>NOTE: These settings will cause RocksDB to consume a lot of memory for block caching
     * and compactions. If you experience out-of-memory problems related to, RocksDB, consider
     * switching back to {@link #SPINNING_DISK_OPTIMIZED}.</b>
     *
     * <p>The following options are set:
     *
     * <ul>
     *   <li>BlockBasedTableConfig.setBlockCacheSize(256 MBytes)
     *   <li>BlockBasedTableConfig.setBlockSize(128 KBytes)
     *   <li>BlockBasedTableConfig.setFilterPolicy(BloomFilter( {@link
     *       RocksDBConfigurableOptions#BLOOM_FILTER_BITS_PER_KEY}, {@link
     *       RocksDBConfigurableOptions#BLOOM_FILTER_BLOCK_BASED_MODE})
     *   <li>setLevelCompactionDynamicLevelBytes(true)
     *   <li>setMaxBackgroundJobs(4)
     *   <li>setMaxBytesForLevelBase(1 GByte)
     *   <li>setMaxOpenFiles(-1)
     *   <li>setMaxWriteBufferNumber(4)
     *   <li>setMinWriteBufferNumberToMerge(3)
     *   <li>setTargetFileSizeBase(256 MBytes)
     *   <li>setWriteBufferSize(64 MBytes)
     * </ul>
     *
     * <p>Enabling use of a Bloom filter here is equivalent to setting {@link
     * RocksDBConfigurableOptions#USE_BLOOM_FILTER}.
     */
    SPINNING_DISK_OPTIMIZED_HIGH_MEM(
            new HashMap<ConfigOption<?>, Object>() {
                private static final long serialVersionUID = 1L;

                {
                    put(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE, MemorySize.parse("256mb"));
                    put(RocksDBConfigurableOptions.BLOCK_SIZE, MemorySize.parse("128kb"));
                    put(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE, true);
                    put(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 4);
                    put(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE, MemorySize.parse("1gb"));
                    put(RocksDBConfigurableOptions.MAX_OPEN_FILES, -1);
                    put(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER, 4);
                    put(RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE, 3);
                    put(
                            RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE,
                            MemorySize.parse("256mb"));
                    put(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE, MemorySize.parse("64mb"));
                    put(RocksDBConfigurableOptions.USE_BLOOM_FILTER, true);
                }
            }),

    /**
     * Pre-defined options for Flash SSDs.
     *
     * <p>This constant configures RocksDB with some options that lead empirically to better
     * performance when the machines executing the system use SSDs.
     *
     * <p>The following options are set:
     *
     * <ul>
     *   <li>setMaxBackgroundJobs(4)
     *   <li>setMaxOpenFiles(-1)
     * </ul>
     */
    FLASH_SSD_OPTIMIZED(
            new HashMap<ConfigOption<?>, Object>() {
                private static final long serialVersionUID = 1L;

                {
                    put(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS, 4);
                    put(RocksDBConfigurableOptions.MAX_OPEN_FILES, -1);
                }
            });

    // ------------------------------------------------------------------------

    /** Settings kept in this pre-defined options. */
    private final Map<String, Object> options;

    PredefinedOptions(Map<ConfigOption<?>, Object> initMap) {
        options = CollectionUtil.newHashMapWithExpectedSize(initMap.size());
        for (Map.Entry<ConfigOption<?>, Object> entry : initMap.entrySet()) {
            options.put(entry.getKey().key(), entry.getValue());
        }
    }

    /**
     * Get a option value according to the pre-defined values. If not defined, return the default
     * value.
     *
     * @param option the option.
     * @param <T> the option value type.
     * @return the value if defined, otherwise return the default value.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    <T> T getValue(ConfigOption<T> option) {
        Object value = options.get(option.key());
        if (value == null) {
            value = option.defaultValue();
        }
        if (value == null) {
            return null;
        }
        return (T) value;
    }
}
