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

package org.apache.flink.state.forst.restore;

import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.forst.ForStDBTtlCompactFiltersManager;
import org.apache.flink.state.forst.ForStNativeMetricMonitor;
import org.apache.flink.state.forst.ForStNativeMetricOptions;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.forstdb.ColumnFamilyDescriptor;
import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;
import org.forstdb.ExportImportFilesMetaData;
import org.forstdb.RocksDB;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.state.forst.ForStOperationUtils.createStateInfo;
import static org.apache.flink.state.forst.ForStOperationUtils.registerKvStateInformation;

/** Utility for creating a ForSt instance either from scratch or from restored local state. */
class ForStHandle implements AutoCloseable {

    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;
    private final DBOptions dbOptions;
    private final Map<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation;
    private final String dbPath;
    private List<ColumnFamilyHandle> columnFamilyHandles;
    private List<ColumnFamilyDescriptor> columnFamilyDescriptors;
    private final ForStNativeMetricOptions nativeMetricOptions;
    private final MetricGroup metricGroup;
    private final ForStDBTtlCompactFiltersManager ttlCompactFiltersManager;
    private final Long writeBufferManagerCapacity;

    private RocksDB db;
    private ColumnFamilyHandle defaultColumnFamilyHandle;
    @Nullable private ForStNativeMetricMonitor nativeMetricMonitor;

    protected ForStHandle(
            Map<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            Path instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            ForStNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            ForStDBTtlCompactFiltersManager ttlCompactFiltersManager,
            Long writeBufferManagerCapacity) {
        this.kvStateInformation = kvStateInformation;
        this.dbPath = instanceRocksDBPath.getPath();
        this.dbOptions = dbOptions;
        this.columnFamilyOptionsFactory = columnFamilyOptionsFactory;
        this.nativeMetricOptions = nativeMetricOptions;
        this.metricGroup = metricGroup;
        this.columnFamilyHandles = new ArrayList<>(1);
        this.columnFamilyDescriptors = Collections.emptyList();
        this.ttlCompactFiltersManager = ttlCompactFiltersManager;
        this.writeBufferManagerCapacity = writeBufferManagerCapacity;
    }

    void openDB() throws IOException {
        loadDb();
    }

    void openDB(
            @Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
            @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            @Nonnull ICloseableRegistry cancelStreamRegistryForRestore)
            throws IOException {
        this.columnFamilyDescriptors = columnFamilyDescriptors;
        this.columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);
        loadDb();
        // Register CF handlers
        for (int i = 0; i < stateMetaInfoSnapshots.size(); i++) {
            getOrRegisterStateColumnFamilyHandle(
                    columnFamilyHandles.get(i),
                    stateMetaInfoSnapshots.get(i),
                    cancelStreamRegistryForRestore);
        }
    }

    private void loadDb() throws IOException {
        db =
                ForStOperationUtils.openDB(
                        dbPath,
                        columnFamilyDescriptors,
                        columnFamilyHandles,
                        ForStOperationUtils.createColumnFamilyOptions(
                                columnFamilyOptionsFactory, "default"),
                        dbOptions);
        // remove the default column family which is located at the first index
        defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
        // init native metrics monitor if configured
        nativeMetricMonitor =
                nativeMetricOptions.isEnabled()
                        ? new ForStNativeMetricMonitor(
                                nativeMetricOptions, metricGroup, db, dbOptions.statistics())
                        : null;
    }

    ForStOperationUtils.ForStKvStateInfo getOrRegisterStateColumnFamilyHandle(
            ColumnFamilyHandle columnFamilyHandle,
            StateMetaInfoSnapshot stateMetaInfoSnapshot,
            ICloseableRegistry cancelStreamRegistryForRestore) {

        ForStOperationUtils.ForStKvStateInfo registeredStateMetaInfoEntry =
                kvStateInformation.get(stateMetaInfoSnapshot.getName());

        if (null == registeredStateMetaInfoEntry) {
            // create a meta info for the state on restore;
            // this allows us to retain the state in future snapshots even if it wasn't accessed
            RegisteredStateMetaInfoBase stateMetaInfo =
                    RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
            if (columnFamilyHandle == null) {
                registeredStateMetaInfoEntry =
                        createStateInfo(
                                stateMetaInfo,
                                db,
                                columnFamilyOptionsFactory,
                                ttlCompactFiltersManager,
                                writeBufferManagerCapacity,
                                cancelStreamRegistryForRestore);
            } else {
                registeredStateMetaInfoEntry =
                        new ForStOperationUtils.ForStKvStateInfo(columnFamilyHandle, stateMetaInfo);
            }

            registerKvStateInformation(
                    kvStateInformation,
                    nativeMetricMonitor,
                    stateMetaInfoSnapshot.getName(),
                    registeredStateMetaInfoEntry);
        } else {
            // TODO: with eager state registration in place, check here for serializer migration
            // strategies.
        }

        return registeredStateMetaInfoEntry;
    }

    /**
     * Registers a new column family and imports data from the given export.
     *
     * @param stateMetaInfoKey info about the state to create.
     * @param cfMetaDataList the data to import.
     */
    void registerStateColumnFamilyHandleWithImport(
            RegisteredStateMetaInfoBase.Key stateMetaInfoKey,
            List<ExportImportFilesMetaData> cfMetaDataList,
            ICloseableRegistry cancelStreamRegistryForRestore) {

        RegisteredStateMetaInfoBase stateMetaInfo =
                stateMetaInfoKey.getRegisteredStateMetaInfoBase();

        Preconditions.checkState(
                !kvStateInformation.containsKey(stateMetaInfo.getName()),
                "Error: stateMetaInfo.name is not unique:" + stateMetaInfo.getName());

        ForStOperationUtils.ForStKvStateInfo stateInfo =
                createStateInfo(
                        stateMetaInfo,
                        db,
                        columnFamilyOptionsFactory,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity,
                        cfMetaDataList,
                        cancelStreamRegistryForRestore);

        registerKvStateInformation(
                kvStateInformation, nativeMetricMonitor, stateMetaInfo.getName(), stateInfo);

        columnFamilyHandles.add(stateInfo.columnFamilyHandle);
    }

    public RocksDB getDb() {
        return db;
    }

    @Nullable
    public ForStNativeMetricMonitor getNativeMetricMonitor() {
        return nativeMetricMonitor;
    }

    public List<ColumnFamilyHandle> getColumnFamilyHandles() {
        return columnFamilyHandles;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
    }

    public Function<String, ColumnFamilyOptions> getColumnFamilyOptionsFactory() {
        return columnFamilyOptionsFactory;
    }

    public ForStDBTtlCompactFiltersManager getTtlCompactFiltersManager() {
        return ttlCompactFiltersManager;
    }

    public Long getWriteBufferManagerCapacity() {
        return writeBufferManagerCapacity;
    }

    public DBOptions getDbOptions() {
        return dbOptions;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(defaultColumnFamilyHandle);
        IOUtils.closeQuietly(nativeMetricMonitor);
        IOUtils.closeQuietly(db);
        // Making sure the already created column family options will be closed
        columnFamilyDescriptors.forEach((cfd) -> IOUtils.closeQuietly(cfd.getOptions()));
    }
}
