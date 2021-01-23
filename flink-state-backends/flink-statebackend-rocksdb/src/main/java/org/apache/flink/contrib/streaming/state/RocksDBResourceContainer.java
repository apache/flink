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

import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteOptions;

import javax.annotation.Nullable;

import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The container for RocksDB resources, including predefined options, option factory and shared
 * resource among instances.
 *
 * <p>This should be the only entrance for {@link RocksDBStateBackend} to get RocksDB options, and
 * should be properly (and necessarily) closed to prevent resource leak.
 */
public final class RocksDBResourceContainer implements AutoCloseable {

    /** The pre-configured option settings. */
    private final PredefinedOptions predefinedOptions;

    /** The options factory to create the RocksDB options. */
    @Nullable private final RocksDBOptionsFactory optionsFactory;

    /**
     * The shared resource among RocksDB instances. This resource is not part of the
     * 'handlesToClose', because the handles to close are closed quietly, whereas for this one, we
     * want exceptions to be reported.
     */
    @Nullable private final OpaqueMemoryResource<RocksDBSharedResources> sharedResources;

    /** The handles to be closed when the container is closed. */
    private final ArrayList<AutoCloseable> handlesToClose;

    public RocksDBResourceContainer() {
        this(PredefinedOptions.DEFAULT, null, null);
    }

    public RocksDBResourceContainer(
            PredefinedOptions predefinedOptions, @Nullable RocksDBOptionsFactory optionsFactory) {
        this(predefinedOptions, optionsFactory, null);
    }

    public RocksDBResourceContainer(
            PredefinedOptions predefinedOptions,
            @Nullable RocksDBOptionsFactory optionsFactory,
            @Nullable OpaqueMemoryResource<RocksDBSharedResources> sharedResources) {

        this.predefinedOptions = checkNotNull(predefinedOptions);
        this.optionsFactory = optionsFactory;
        this.sharedResources = sharedResources;
        this.handlesToClose = new ArrayList<>();
    }

    /** Gets the RocksDB {@link DBOptions} to be used for RocksDB instances. */
    public DBOptions getDbOptions() {
        // initial options from pre-defined profile
        DBOptions opt = predefinedOptions.createDBOptions(handlesToClose);
        handlesToClose.add(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createDBOptions(opt, handlesToClose);
        }

        // add necessary default options
        opt = opt.setCreateIfMissing(true);

        // if sharedResources is non-null, use the write buffer manager from it.
        if (sharedResources != null) {
            opt.setWriteBufferManager(sharedResources.getResourceHandle().getWriteBufferManager());
        }

        return opt;
    }

    /**
     * Gets write buffer manager capacity.
     *
     * @return the capacity of the write buffer manager, or null if write buffer manager is not
     *     enabled.
     */
    public Long getWriteBufferManagerCapacity() {
        if (sharedResources == null) {
            return null;
        }

        return sharedResources.getResourceHandle().getWriteBufferManagerCapacity();
    }

    /** Gets the RocksDB {@link ColumnFamilyOptions} to be used for all RocksDB instances. */
    public ColumnFamilyOptions getColumnOptions() {
        // initial options from pre-defined profile
        ColumnFamilyOptions opt = predefinedOptions.createColumnOptions(handlesToClose);
        handlesToClose.add(opt);

        // add user-defined options, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createColumnOptions(opt, handlesToClose);
        }

        // if sharedResources is non-null, use the block cache from it and
        // set necessary options for performance consideration with memory control
        if (sharedResources != null) {
            final RocksDBSharedResources rocksResources = sharedResources.getResourceHandle();
            final Cache blockCache = rocksResources.getCache();
            TableFormatConfig tableFormatConfig = opt.tableFormatConfig();
            BlockBasedTableConfig blockBasedTableConfig;
            if (tableFormatConfig == null) {
                blockBasedTableConfig = new BlockBasedTableConfig();
            } else {
                Preconditions.checkArgument(
                        tableFormatConfig instanceof BlockBasedTableConfig,
                        "We currently only support BlockBasedTableConfig When bounding total memory.");
                blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
            }
            blockBasedTableConfig.setBlockCache(blockCache);
            blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
            blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
            blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
            opt.setTableFormatConfig(blockBasedTableConfig);
        }

        return opt;
    }

    /** Gets the RocksDB {@link WriteOptions} to be used for write operations. */
    public WriteOptions getWriteOptions() {
        // Disable WAL by default
        WriteOptions opt = new WriteOptions().setDisableWAL(true);
        handlesToClose.add(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createWriteOptions(opt, handlesToClose);
        }

        return opt;
    }

    /** Gets the RocksDB {@link ReadOptions} to be used for read operations. */
    public ReadOptions getReadOptions() {
        // We ensure total order seek by default to prevent user misuse, see FLINK-17800 for more
        // details
        ReadOptions opt = RocksDBOperationUtils.createTotalOrderSeekReadOptions();
        handlesToClose.add(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createReadOptions(opt, handlesToClose);
        }

        return opt;
    }

    RocksDBNativeMetricOptions getMemoryWatcherOptions(
            RocksDBNativeMetricOptions defaultMetricOptions) {
        return optionsFactory == null
                ? defaultMetricOptions
                : optionsFactory.createNativeMetricsOptions(defaultMetricOptions);
    }

    PredefinedOptions getPredefinedOptions() {
        return predefinedOptions;
    }

    @Nullable
    RocksDBOptionsFactory getOptionsFactory() {
        return optionsFactory;
    }

    @Override
    public void close() throws Exception {
        handlesToClose.forEach(IOUtils::closeQuietly);
        handlesToClose.clear();

        if (sharedResources != null) {
            sharedResources.close();
        }
    }
}
