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

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.state.forst.ForStNativeMetricMonitor;
import org.apache.flink.state.forst.ForStNativeMetricOptions;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/** Utility for creating a ForSt instance either from scratch or from restored local state. */
class ForStHandle implements AutoCloseable {

    private final Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory;
    private final DBOptions dbOptions;
    private final String dbPath;
    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;
    private final ForStNativeMetricOptions nativeMetricOptions;
    private final MetricGroup metricGroup;

    private RocksDB db;
    private ColumnFamilyHandle defaultColumnFamilyHandle;
    @Nullable private ForStNativeMetricMonitor nativeMetricMonitor;

    protected ForStHandle(
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            ForStNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup) {
        this.dbPath = instanceRocksDBPath.getAbsolutePath();
        this.dbOptions = dbOptions;
        this.columnFamilyOptionsFactory = columnFamilyOptionsFactory;
        this.nativeMetricOptions = nativeMetricOptions;
        this.metricGroup = metricGroup;
        this.columnFamilyHandles = new ArrayList<>(1);
        this.columnFamilyDescriptors = Collections.emptyList();
    }

    void openDB() throws IOException {
        loadDb();
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

    public RocksDB getDb() {
        return db;
    }

    @Nullable
    public ForStNativeMetricMonitor getNativeMetricMonitor() {
        return nativeMetricMonitor;
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
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
