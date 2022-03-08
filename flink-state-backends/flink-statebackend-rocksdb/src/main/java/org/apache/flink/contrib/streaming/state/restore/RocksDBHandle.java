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

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricMonitor;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Utility for creating a RocksDB instance either from scratch or from restored local state. This
 * will also register {@link RocksDbKvStateInfo} when using {@link #openDB(List, List, Path)}.
 */
class RocksDBHandle implements AutoCloseable {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;
    private final DBOptions dbOptions;
    private final Map<String, RocksDbKvStateInfo> kvStateInformation;
    private final String dbPath;
    private List<ColumnFamilyHandle> columnFamilyHandles;
    private List<ColumnFamilyDescriptor> columnFamilyDescriptors;
    private final RocksDBNativeMetricOptions nativeMetricOptions;
    private final MetricGroup metricGroup;
    // Current places to set compact filter into column family options:
    // - Incremental restore
    //   - restore with rescaling
    //     - init from a certain sst: #createAndRegisterColumnFamilyDescriptors when prepare files,
    // before db open
    //     - data ingestion after db open: #getOrRegisterStateColumnFamilyHandle before creating
    // column family
    //   - restore without rescaling: #createAndRegisterColumnFamilyDescriptors when prepare files,
    // before db open
    // - Full restore
    //   - data ingestion after db open: #getOrRegisterStateColumnFamilyHandle before creating
    // column family
    private final RocksDbTtlCompactFiltersManager ttlCompactFiltersManager;

    private RocksDB db;
    private ColumnFamilyHandle defaultColumnFamilyHandle;
    private RocksDBNativeMetricMonitor nativeMetricMonitor;
    private final Long writeBufferManagerCapacity;

    protected RocksDBHandle(
            Map<String, RocksDbKvStateInfo> kvStateInformation,
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            RocksDBNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            Long writeBufferManagerCapacity) {
        this.kvStateInformation = kvStateInformation;
        this.dbPath = instanceRocksDBPath.getAbsolutePath();
        this.dbOptions = dbOptions;
        this.columnFamilyOptionsFactory = columnFamilyOptionsFactory;
        this.nativeMetricOptions = nativeMetricOptions;
        this.metricGroup = metricGroup;
        this.ttlCompactFiltersManager = ttlCompactFiltersManager;
        this.columnFamilyHandles = new ArrayList<>(1);
        this.columnFamilyDescriptors = Collections.emptyList();
        this.writeBufferManagerCapacity = writeBufferManagerCapacity;
    }

    void openDB() throws IOException {
        loadDb();
    }

    void openDB(
            @Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
            @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            @Nonnull Path restoreSourcePath)
            throws IOException {
        this.columnFamilyDescriptors = columnFamilyDescriptors;
        this.columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);
        restoreInstanceDirectoryFromPath(restoreSourcePath);
        loadDb();
        // Register CF handlers
        for (int i = 0; i < stateMetaInfoSnapshots.size(); i++) {
            getOrRegisterStateColumnFamilyHandle(
                    columnFamilyHandles.get(i), stateMetaInfoSnapshots.get(i));
        }
    }

    private void loadDb() throws IOException {
        db =
                RocksDBOperationUtils.openDB(
                        dbPath,
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        RocksDBOperationUtils.createColumnFamilyOptions(
                                columnFamilyOptionsFactory, "default"),
                        dbOptions);
        // remove the default column family which is located at the first index
        defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
        // init native metrics monitor if configured
        nativeMetricMonitor =
                nativeMetricOptions.isEnabled()
                        ? new RocksDBNativeMetricMonitor(nativeMetricOptions, metricGroup, db)
                        : null;
    }

    RocksDbKvStateInfo getOrRegisterStateColumnFamilyHandle(
            ColumnFamilyHandle columnFamilyHandle, StateMetaInfoSnapshot stateMetaInfoSnapshot) {

        RocksDbKvStateInfo registeredStateMetaInfoEntry =
                kvStateInformation.get(stateMetaInfoSnapshot.getName());

        if (null == registeredStateMetaInfoEntry) {
            // create a meta info for the state on restore;
            // this allows us to retain the state in future snapshots even if it wasn't accessed
            RegisteredStateMetaInfoBase stateMetaInfo =
                    RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
            if (columnFamilyHandle == null) {
                registeredStateMetaInfoEntry =
                        RocksDBOperationUtils.createStateInfo(
                                stateMetaInfo,
                                db,
                                columnFamilyOptionsFactory,
                                ttlCompactFiltersManager,
                                writeBufferManagerCapacity);
            } else {
                registeredStateMetaInfoEntry =
                        new RocksDbKvStateInfo(columnFamilyHandle, stateMetaInfo);
            }

            RocksDBOperationUtils.registerKvStateInformation(
                    kvStateInformation,
                    nativeMetricMonitor,
                    stateMetaInfoSnapshot.getName(),
                    registeredStateMetaInfoEntry);
        } else {
            // TODO with eager state registration in place, check here for serializer migration
            // strategies
        }

        return registeredStateMetaInfoEntry;
    }

    /**
     * This recreates the new working directory of the recovered RocksDB instance and links/copies
     * the contents from a local state.
     */
    private void restoreInstanceDirectoryFromPath(Path source) throws IOException {
        final Path instanceRocksDBDirectory = Paths.get(dbPath);
        final Path[] files = FileUtils.listDirectory(source);

        if (!new File(dbPath).mkdirs()) {
            String errMsg = "Could not create RocksDB data directory: " + dbPath;
            logger.error(errMsg);
            throw new IOException(errMsg);
        }

        for (Path file : files) {
            final String fileName = file.getFileName().toString();
            final Path targetFile = instanceRocksDBDirectory.resolve(fileName);
            if (fileName.endsWith(SST_FILE_SUFFIX)) {
                try {
                    // hardlink'ing the immutable sst-files.
                    Files.createLink(targetFile, file);
                    continue;
                } catch (IOException ioe) {
                    final String logMessage =
                            String.format(
                                    "Could not hard link sst file %s. Trying to copy it over. This might "
                                            + "increase the recovery time. In order to avoid this, configure "
                                            + "RocksDB's working directory and the local state directory to be on the same volume.",
                                    fileName);
                    if (logger.isDebugEnabled()) {
                        logger.debug(logMessage, ioe);
                    } else {
                        logger.info(logMessage);
                    }
                }
            }

            // true copy for all other files and files that could not be hard linked.
            Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public RocksDB getDb() {
        return db;
    }

    public RocksDBNativeMetricMonitor getNativeMetricMonitor() {
        return nativeMetricMonitor;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
    }

    public List<ColumnFamilyHandle> getColumnFamilyHandles() {
        return columnFamilyHandles;
    }

    public RocksDbTtlCompactFiltersManager getTtlCompactFiltersManager() {
        return ttlCompactFiltersManager;
    }

    public Long getWriteBufferManagerCapacity() {
        return writeBufferManagerCapacity;
    }

    public Function<String, ColumnFamilyOptions> getColumnFamilyOptionsFactory() {
        return columnFamilyOptionsFactory;
    }

    public DBOptions getDbOptions() {
        return dbOptions;
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(defaultColumnFamilyHandle);
        IOUtils.closeQuietly(nativeMetricMonitor);
        IOUtils.closeQuietly(db);
        // Making sure the already created column family options will be closed
        columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));
    }
}
